namespace OneBRC

module Program =

  open System.Diagnostics

  [<EntryPoint>]
  let main argv =
    let idealChunkLength = 256 * 1024 * 1024
    let filePath = argv[0]
    let stopwatch = Stopwatch.StartNew ()
    Challenge.runMemoryMapped idealChunkLength filePath
    stopwatch.Stop ()
    printfn "Elapsed: %A" stopwatch.Elapsed
    0
