namespace OneBRC

module Program =

  open System.Diagnostics

  [<EntryPoint>]
  let main argv =
    let idealChunkLength = 256 * 1024 * 1024
    let filePath = argv[0]
    let stopwatch = Stopwatch.StartNew ()
    let results = Challenge.runMemoryMapped idealChunkLength filePath
    let t0 = stopwatch.Elapsed
    Challenge.print results
    stopwatch.Stop ()
    let t1 = stopwatch.Elapsed
    printfn "Processing: %A" t0
    printfn "Processing and printing: %A" t1
    0
