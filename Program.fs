namespace OneBRC

module Program =

  open System.Diagnostics

  [<EntryPoint>]
  let main argv =
    let idealChunkLength = 256 * 1024 * 1024
    let filePath =
      if not <| Array.isEmpty argv then argv[0]
      else  "/Users/johana/Programming/FSharp/1BR/data/measurements.txt"
    let stopwatch = Stopwatch.StartNew ()
    let results = Challenge.runMemoryMapped idealChunkLength filePath
    stopwatch.Stop ()
    let t0 = stopwatch.Elapsed
    stopwatch.Start ()
    Challenge.print results
    stopwatch.Stop ()
    let t1 = stopwatch.Elapsed
    printfn "Processing: %A" t0
    printfn "Processing and printing: %A" t1
    0
