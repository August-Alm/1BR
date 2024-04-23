namespace OneBRC

module Program =

  open System.Diagnostics

  [<EntryPoint>]
  let main argv =
    let idealChunkLength = 256 * 1024 * 1024
    let filePath =
      if not <| Array.isEmpty argv then argv[0]
      else "/Users/johana/Programming/FSharp/1BR/data/measurements.txt"
    let stopwatch = Stopwatch.StartNew ()
    filePath
    |> Challenge.mmfInput
    |> Challenge.run idealChunkLength
    |> Challenge.print
    stopwatch.Stop ()
    printfn "Total time: %A" stopwatch.Elapsed
    0
