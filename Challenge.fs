namespace OneBRC

[<RequireQualifiedAccess>]
module Challenge =

  open System.IO.MemoryMappedFiles
  open Microsoft.FSharp.NativeInterop
  open System.IO
  open System.Threading

  let print (css : (struct (City * Stat)) seq) =
    printf "{"
    let mutable delim = ""
    for (struct (city, stat)) in css do
      printf "%s%A=%A" delim city stat
      delim <- ", "
    printfn "}"
    
  type Chunk (input : Input) =
    let cityStats = CityStats.create 1024

    member _.CityStats = cityStats

    member _.Run () =
      let mutable input = input
      while input.Length > 0L do
        let city = City.parse &input
        let temp = Temp.parse &input
        CityStats.add cityStats city temp

  let private runChunk (chunk : Input) =
    let cityStats = CityStats.create 1024
    let mutable input = chunk
    while input.Length > 0L do
      let city = City.parse &input
      let temp = Temp.parse &input
      CityStats.add cityStats city temp
    cityStats

  let run' idealChunkLength input =
    let chunks =
      input
      |> Input.chunkify idealChunkLength
      |> Seq.map Chunk
    let threads =
      chunks
      |> Seq.map (fun c -> Thread (c.Run))
      |> Seq.toArray
    threads |> Array.iter (fun t -> t.Start ())
    threads |> Array.iter (fun t -> t.Join ())
    chunks
    |> Seq.map (fun c -> c.CityStats)
    |> CityStats.merge

  let run idealChunkLength input =
    input
    |> Input.chunkify idealChunkLength
    |> Seq.toArray
#if DEBUG
    |> Array.map runChunk
#else
    |> Array.Parallel.map runChunk
#endif
    |> CityStats.merge

  let runMemoryMapped idealChunkLength filePath  =
    use mmf = MemoryMappedFile.CreateFromFile (filePath, FileMode.Open)
    use accessor = mmf.CreateViewAccessor ()
    use accessorHandle = accessor.SafeMemoryMappedViewHandle
    let fileLength = accessor.Capacity
    let mutable ptr = NativePtr.nullPtr<byte>
    accessorHandle.AcquirePointer &ptr
    //run idealChunkLength (Input (ptr, fileLength)) 
    run' idealChunkLength (Input (ptr, fileLength)) 