namespace OneBRC

[<AutoOpen>]
module Util =

  open System
  open System.Collections.Generic
  open Microsoft.FSharp.NativeInterop
  open System.Runtime.InteropServices

  module private NativePtr =

    let inline cast<'t when 't : unmanaged> (ptr : nativeptr<byte>) =
      NativePtr.ofVoidPtr<'t> (NativePtr.toVoidPtr ptr)
    
    let inline readAt (ptr : nativeptr<'t>) (offset : int) =
      NativePtr.read<'t> (NativePtr.add ptr offset)


  /// Represents a portion of a file. Morally equivalent to `ReadOnlySpan<byte>`
  /// but allows for int64 lengths.
  [<Struct>]
  type Input =
    val mutable private ptr : nativeptr<byte>
    val mutable private length : int64
    new (ptr : nativeptr<byte>, length : int64) = { ptr = ptr; length = length }

    member this.Ptr = this.ptr
    member this.Length = this.length
    member this.ReadOnlySpan = ReadOnlySpan<byte> (NativePtr.toVoidPtr this.ptr, int this.length)
    member this.Slice (start : int, length : int64) = Input (NativePtr.add this.ptr start, length)
    member this.Slice (start : int) = Input (NativePtr.add this.ptr start, this.length - int64 start)

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


  module private Parse =

    /// Mutates `input` to the tail that follows the first occurrence of `b`
    /// and returns the head that precedes it. If `b` is not found, it returns
    /// `input` unchanged.
    let inline readTo (b : byte) (input : Input byref) =
      let mutable i = 0
      let mutable found = false
      while not found && int64 i < input.Length do
        found <- NativePtr.readAt input.Ptr i = b
        if not found then
          i <- i + 1
      if not found then
        input.ReadOnlySpan
      else
        let tmp = input.ReadOnlySpan.Slice (0, int i)
        input <- input.Slice (i + 1)
        tmp

  [<RequireQualifiedAccess>]
  module Temp =

    let inline private asTemp init (span : ReadOnlySpan<byte>) =
      let mutable n = init
      for b in span.Slice 1 do
        if b <> '.'B then
          n <- n * 10 + int (b - '0'B)
      n

    /// Reads a temperature from `input` up to the first newline, and mutates
    /// `input` to the tail that follows it. The temperature is returned as an
    /// integer, with the decimal point implied before the last digit.
    let parse (input : Input byref) =
      let span = Parse.readTo '\n'B &input
      let mutable b = span[0]
      let isNeg = (b = '-'B)
      if isNeg then -(asTemp 0 span)
      else asTemp (int (b - '0'B)) span
    

  /// Represents a city name. Equivalent to `ReadOnlySpan<byte>` but with custom
  /// hashing, equality and comparison.
  [<Struct; CustomEquality; CustomComparison>]
  type City (ptr : nativeptr<byte>, length : int) =

    new (span : ReadOnlySpan<byte>) = use p = fixed span in City (p, span.Length)

    member _.ReadOnlySpan = ReadOnlySpan<byte> (NativePtr.toVoidPtr ptr, length)

    member this.Equals (other : City) = this.ReadOnlySpan.SequenceEqual other.ReadOnlySpan
    
    member this.CompareTo (other : City) = this.ReadOnlySpan.SequenceCompareTo other.ReadOnlySpan

    override _.ToString () = String (NativePtr.cast ptr, 0, length, Text.Encoding.UTF8)

    override _.GetHashCode () =
      let prime = 16777619u
      if length >= 3 then
        let k = NativePtr.read<uint32> (NativePtr.cast ptr)
        int ((k * prime) ^^^ (uint length))
      else
        let k = uint (NativePtr.read<uint16> (NativePtr.cast ptr))
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
    let parse (input : Input byref) = City (Parse.readTo ';'B &input)


  /// Represents a temperature statistic, with minimum, mean and maximum
  /// properties, and an `Add` method to update the statistics with a new
  /// temperature, and a `Merge` method to merge two statistics.
  [<Struct>]
  type Stat =
    val mutable private Min : int
    val mutable private Max : int
    val mutable private Sum : int
    val mutable private Count : int

    private new (min : int, max : int, sum : int, count : int) =
      { Min = min; Max = max; Sum = sum; Count = count }

    member this.Minimum = (double this.Min) / 10.0
    member this.Maximum = (double this.Max) / 10.0
    member this.Mean = (double this.Sum / 10.0) / (double this.Count)

    /// We shall store temperature floating point numbers with one decimal digit
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
    
    override this.ToString () =
      sprintf "%.1f/%.1f/%.1f" this.Minimum this.Mean this.Maximum

    /// Merges another statistics object into this one.
    member this.Merge (other : Stat) =
      this.Min <- min this.Min other.Min
      this.Max <- max this.Max other.Max
      this.Sum <- this.Sum + other.Sum
      this.Count <- this.Count + other.Count

    
  /// A dictionary of city names to temperature statistics.
  type CityStats = Dictionary<City, Stat>
  
  [<RequireQualifiedAccess>]
  module CityStats =

    let create (capacity : int) = Dictionary<City, Stat> capacity

    let privateAddMerge (cityStats : CityStats) (city : City) (stat : Stat) =
      let mutable exists = false
      let stat' = CollectionsMarshal.GetValueRefOrAddDefault (cityStats, city, &exists)
      if exists then stat'.Merge stat
      else cityStats[city] <- stat

    /// Merges a sequence of city statistics dictionaries into a single sequence
    /// sorted by city name.
    let merge (cityStatss : CityStats seq) : (struct (City * Stat)) seq =
      let result = create 1024
      for cityStats in cityStatss do
        for KeyValue (city, stat) in cityStats do
          privateAddMerge result city stat
      result
      |> Seq.map (fun (KeyValue (city, stat)) -> struct (city, stat))
      |> Seq.sortBy (fun (struct (city, _)) -> city)

    /// Adds a new temperature to the statistics for a city.
    let add (cityState : CityStats) (city : City) (temp : int) =
      let mutable exists = false
      let mutable stat : byref<Stat> = &CollectionsMarshal.GetValueRefOrAddDefault (cityState, city, &exists)
      stat.Add (temp, not exists)

