namespace OneBRC

[<RequireQualifiedAccess>]
module Challenge =

  open System.IO.MemoryMappedFiles
  open Microsoft.FSharp.NativeInterop
  open System.IO
  open System.Threading

  module Array =

    type private Mapping<'a, 'b> (f : 'a -> 'b, x : 'a) =
      let mutable result = Unchecked.defaultof<'b>
      member _.Run () = result <- f x
      member _.Result = result
    
    /// Morally equivalent to `Array.Parallel.map` but slightly faster for use case.
    let mapParallel (f : 'a -> 'b) (xs : 'a array) : 'b array =
      let mappings = xs |> Array.map (fun x -> Mapping (f, x))
      let threads = mappings |> Array.map (fun m -> Thread (fun () -> m.Run ()))
      for t in threads do t.Start ()
      for t in threads do t.Join ()
      mappings |> Array.map _.Result


  let print (cityStats : CityStats) =
    printf "{"
    let mutable delim = ""
    cityStats
    |> Seq.sortBy _.Key
    |> Seq.iter
      (fun (KeyValue (city, stat)) ->
        printf "%s%A=%A" delim city stat
        delim <- ", ")
    printfn "}"
    
  let private runChunk (chunk : Input) =
    let cityStats = CityStats.create 1024
    let mutable input = chunk
    while input.Length > 0L do
      let city = City.parse &input
      let temp = Temp.parse &input
      CityStats.add cityStats city temp
    cityStats
  
  let run idealChunkLength input =
    input
    |> Input.chunkify idealChunkLength
#if DEBUG
    |> Array.map runChunk
#else
    |> Array.mapParallel runChunk
#endif
    |> CityStats.merge
  
  let mmfInput filePath =
    use mmf = MemoryMappedFile.CreateFromFile (filePath, FileMode.Open)
    use stream = new FileStream (filePath, FileMode.Open)
    let fileLength = stream.Length
    use accessor = mmf.CreateViewAccessor (0, fileLength, MemoryMappedFileAccess.Read)
    use accessorHandle = accessor.SafeMemoryMappedViewHandle
    let mutable ptr = NativePtr.nullPtr<byte>
    accessorHandle.AcquirePointer &ptr
    Input (ptr, fileLength)
