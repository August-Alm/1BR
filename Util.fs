namespace OneBRC

[<AutoOpen>]
module Util =

  open System
  open System.Numerics
  open System.Collections.Generic
  open Microsoft.FSharp.NativeInterop
  open System.Runtime.InteropServices

  [<RequireQualifiedAccess>]
  module private NativePtr =

    let inline cast<'t when 't : unmanaged> (ptr : nativeptr<byte>) =
      NativePtr.ofVoidPtr<'t> (NativePtr.toVoidPtr ptr)
    
    let inline readAt (ptr : nativeptr<'t>) (offset : int) =
      NativePtr.read<'t> (NativePtr.add ptr offset)


  /// Represents a portion of a file. Morally equivalent to `ReadOnlySpan<byte>`
  /// but allows for int64 lengths.
  [<Struct>]
  type Input =
    val mutable Ptr : nativeptr<byte>
    val mutable Length : int64
    new (ptr : nativeptr<byte>, length : int64) = { Ptr = ptr; Length = length }

    member inline this.Shift (offset : int) =
      this.Ptr <- NativePtr.add this.Ptr offset
      this.Length <- this.Length - int64 offset

  [<RequireQualifiedAccess>]
  module Input =

    /// Splits `input` into chunks of approximately `idealChunkLength` bytes,
    /// each ending with a newline.
    let chunkify (idealChunkLength : int) (input : Input) =
      let chunks = ResizeArray<Input>()
      let mutable offset = 0L
      let mutable p = input.Ptr
      let length = input.Length
      while offset < length do
        let mutable chunkLength = int ( min (length - offset) (int64 idealChunkLength - 256L))
        let mutable pNextChunk = NativePtr.add p chunkLength
        offset <- offset + int64 chunkLength
        // Find next '\n'.
        while (offset < length) && (NativePtr.read pNextChunk <> '\n'B) do
          pNextChunk <- NativePtr.add pNextChunk 1
          chunkLength <- chunkLength + 1
          offset <- offset + 1L
        // Move beyond the '\n'.
        if offset < length then
          pNextChunk <- NativePtr.add pNextChunk 1
          chunkLength <- chunkLength + 1
          offset <- offset + 1L
        // Output previous chunk.
        chunks.Add (Input (p, chunkLength))
        p <- pNextChunk
      Array.ofSeq chunks

  /// Represents a city name. Equivalent to `ReadOnlySpan<byte>` but with custom
  /// hashing, equality and comparison.
  [<Struct; CustomEquality; CustomComparison>]
  type City (ptr : nativeptr<byte>, length : int) =

    member _.ReadOnlySpan = ReadOnlySpan<byte> (NativePtr.toVoidPtr ptr, length)

    member this.Equals (other : City) = this.ReadOnlySpan.SequenceEqual other.ReadOnlySpan
    
    member this.CompareTo (other : City) = this.ReadOnlySpan.SequenceCompareTo other.ReadOnlySpan

    override _.ToString () = String (NativePtr.cast ptr, 0, length, Text.Encoding.UTF8)

    override _.GetHashCode () =
      let prime = 16777619u
      let k = NativePtr.read<uint32> (NativePtr.cast ptr)
      int ((k * prime) ^^^ (uint length))
    
    override this.Equals (obj : obj) =
      match obj with
      | :? City as other -> this.Equals other
      | _ -> false

    interface IEquatable<City> with
      member this.Equals (other : City) = this.ReadOnlySpan.SequenceEqual other.ReadOnlySpan
    
    interface IComparable<City> with
      member this.CompareTo (other : City) = this.ReadOnlySpan.SequenceCompareTo other.ReadOnlySpan
    
    interface IComparable with
      member this.CompareTo (obj : obj) =
        match obj with
        | :? City as other -> this.CompareTo other
        | _ -> invalidArg "obj" "obj is not a City"
    
  [<RequireQualifiedAccess>]
  module City =

    /// Reads a city name from `input` up to the first semicolon, and mutates
    /// `input` to the tail that follows it.
    let parse (input : Input byref) =
      let mutable i = 0
      let mutable found = false
      while not found do
        found <- (NativePtr.readAt input.Ptr i) = ';'B
        if not found then
          i <- i + 1
      let city = City (input.Ptr, i)
      input.Shift (i + 1)
      city


  [<RequireQualifiedAccess>]
  module Temp =

    let private MAGIC_MULTIPLIER = 100L * 0x1000000L + 10L * 0x10000L + 1L
    let private DOT_DETECTOR = 0x10101000L
    let private ASCII_TO_DIGIT_MASK = 0x0F000F0F00L

    /// Reads a temperature from `input` and mutates `input` to the tail that
    /// follows it. Temperatures are fixed-precision floating point numbers
    /// (with one decimal digit) encoded as the corresponding 10-times integer.
    /// See "https://questdb.io/blog/1brc-merykittys-magic-swar/" for the algorithm
    let parse (input : Input byref) =
      let u = NativePtr.read<int64> (NativePtr.cast input.Ptr)
      let negated = ~~~u
      let broadcastSign = (negated <<< 59) >>> 63
      let maskToRemoveSign = ~~~(broadcastSign &&& 0xFF)
      let withSignRemoved = u &&& maskToRemoveSign
      let dotPos = BitOperations.TrailingZeroCount (negated &&& DOT_DETECTOR)
      let aligned = withSignRemoved <<< (28 - dotPos)
      let digits = aligned &&& ASCII_TO_DIGIT_MASK
      let absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) &&& 0x3FFL
      let temp = (absValue ^^^ broadcastSign) - broadcastSign
      let nextLineStart = (dotPos >>> 3) + 3
      input.Shift nextLineStart
      int temp



  /// Represents a temperature statistic, with minimum, mean and maximum
  /// properties, and an `Add` method to update the statistics with a new
  /// temperature, and a `Merge` method to merge two statistics.
  [<Struct>]
  type Stat =
    val mutable private Min : int
    val mutable private Max : int
    val mutable private Sum : int
    val mutable private Count : int

    member this.Minimum = single this.Min / 10.0f
    member this.Maximum = single this.Max / 10.0f
    member this.Mean = single this.Sum / single (10 * this.Count)

    override this.ToString () =
      sprintf "%.1f/%.1f/%.1f" this.Minimum this.Mean this.Maximum

    /// We store temperature floating point numbers with one decimal digit
    /// as integers. This method adds a new such temperature to the statistics.
    member this.Add (tmp : int, set : bool) =
      if set then
        this.Min <- tmp
        this.Max <- tmp
        this.Sum <- tmp
        this.Count <- 1
      else
        this.Min <- min this.Min tmp
        this.Max <- max this.Max tmp
        this.Sum <- this.Sum + tmp
        this.Count <- this.Count + 1
    
    /// Merges another statistics object into this one.
    member this.Merge (other : Stat, set : bool) =
      if set then
        this.Min <- min this.Min other.Min
        this.Max <- max this.Max other.Max
        this.Sum <- this.Sum + other.Sum
        this.Count <- this.Count + other.Count
      else
        this <- other

    
  /// A dictionary of city names to temperature statistics.
  type CityStats = Dictionary<City, Stat>
  
  [<RequireQualifiedAccess>]
  module CityStats =

    let create (capacity : int) = Dictionary<City, Stat> capacity

    let inline private addMerge (cityStats : CityStats) (city : City) (stat : Stat) =
      let mutable exists = false
      let mutable statRef : byref<Stat> = &CollectionsMarshal.GetValueRefOrAddDefault (cityStats, city, &exists)
      statRef.Merge (stat, exists)

    /// Merges a sequence of city statistics dictionaries into a single sequence
    /// sorted by city name.
    let merge (cityStatss : CityStats seq) =
      let result = create 4096
      for cityStats in cityStatss do
        for KeyValue (city, stat) in cityStats do
          addMerge result city stat
      result

    /// Adds a new temperature to the statistics for a city.
    let add (cityState : CityStats) (city : City) (temp : int) =
      let mutable exists = false
      let mutable statRef : byref<Stat> = &CollectionsMarshal.GetValueRefOrAddDefault (cityState, city, &exists)
      statRef.Add (temp, not exists)
