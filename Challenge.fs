namespace OneBRC

[<RequireQualifiedAccess>]
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

  let runMemoryMapped idealChunkLength filePath  =
    use mmf = MemoryMappedFile.CreateFromFile (filePath, FileMode.Open)
    use accessor = mmf.CreateViewAccessor ()
    use accessorHandle = accessor.SafeMemoryMappedViewHandle
    let fileLength = accessor.Capacity
    let mutable ptr = NativePtr.nullPtr<byte>
    accessorHandle.AcquirePointer &ptr
    run idealChunkLength (Input (ptr, fileLength)) 