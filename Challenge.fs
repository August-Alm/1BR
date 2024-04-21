namespace OneBRC

module Challenge =

  open System.IO.MemoryMappedFiles
  open Microsoft.FSharp.NativeInterop
  open System.IO

  let print (css : (struct (City * Stat)) seq) =
    printf "{"
    let mutable delim = ""
    for (struct (city, stat)) in css do
      printf "%s%A=%A" delim city stat
      delim <- ", "
    printfn "}"
    
  let inline private runChunk (chunk : Input) =
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
    |> Array.Parallel.map runChunk 
    |> CityStats.merge
    |> print

  let runMemoryMapped idealChunkLength filePath  =
    let mmf = MemoryMappedFile.CreateFromFile (filePath, FileMode.Open)
    let accessor = mmf.CreateViewAccessor ()
    let fileLength = accessor.Capacity
    let mutable ptr = NativePtr.nullPtr<byte>
    let accessorHandle = accessor.SafeMemoryMappedViewHandle
    accessorHandle.AcquirePointer &ptr
    run idealChunkLength (Input (ptr, fileLength)) 
    accessorHandle.ReleasePointer ()
    accessorHandle.Dispose ()
    mmf.Dispose ()