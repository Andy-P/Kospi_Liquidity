open System
open System.IO 


let srcDir = __SOURCE_DIRECTORY__

let dataPath = Path.Combine(srcDir,   "../futures/csv by contract/Lazy Read Test/output/20130913/")
let outputPath = Path.Combine(srcDir, "../futures/csv by contract/Lazy Read Test/output/")

let testDataPath = Path.Combine(dataPath,"kse-kospi200_2013-12_20131021_27270.csv")
let testDataPathLarge = Path.Combine(dataPath,"kse-kospi200_2013-12_20131212.csv")

let parseFile path = 

    let parseLine (x:string[])  = 
            (DateTime.Parse(x.[0]), 
             Int32.Parse(x.[12]),
             Int32.Parse(x.[13]), 
             Int32.Parse(x.[14]),
             Int32.Parse(x.[15]),
             Int32.Parse(x.[10]),
             Int32.Parse(x.[20])
             ) 

    let parseTextFile = 
        File.ReadLines(path)
        |> Seq.map(fun x -> x.Split(char(","))) 
        |> Seq.map(fun x -> parseLine x)
        |> Seq.filter(fun (_,_,_,_,_,x,y) ->  (x <> 5) || (y <> 5) )
        |> Seq.map(fun (dt,b,a,l,d,_,_) ->  dt,b,a,l,d)
        |> Seq.toArray

    let seqStatsData =
        parseTextFile
        |> Seq.append (seq { yield parseTextFile |>  Seq.head }) 
        |> Seq.pairwise 
        |> Seq.map(fun  (x , y) -> 
                let changed = (x = y)
                let (date1,bid1,ask1,last1,depth1) =  x
                let (date2,bid2,ask2,last2,depth2) =  y
                let equal = (bid1,ask1,last1,depth1) = (bid2,ask2,last2,depth2)
                date2, bid2, ask2, last2, depth2, (date2.Subtract(date1)).TotalMilliseconds, equal )
        |> Seq.toArray

    let sumStatsOfDups = 
            
            let getSize bid ask = if bid >= ask then (bid, true) else (ask, false)  
              
            let (date, bid, ask, last, depth, tSpan, equal) = (seqStatsData |> Seq.head)
            let size, isBid = getSize bid ask
            let first = (date, bid, ask, size, isBid, last, depth, tSpan, 0)

            let foldedDups = 
                    seqStatsData
                    |> Seq.fold(fun acc x  -> 
                        let date2, bid2, ask2, last2, depth2, tSpan2, equal = x 
                        let y, xs = acc 
                        let (date, bid, ask, size, isBid, last, depth, tSpan, cnt) = y
                        match equal with
                            | true -> (date, bid, ask, size, isBid, last, depth, (tSpan + tSpan2), (cnt + 1)), xs 
                            | false -> let size, isBid = getSize bid2 ask2
                                       (date2, bid2, ask2, size, isBid, last2, depth2, tSpan2, 1), y::xs) (first, [first])

            (snd foldedDups) |> List.rev |> List.tail |>  List.toArray

    sumStatsOfDups

let byDepth = (fun (_, _, _, _, _, _, depth, _, _) -> depth)
let bySize  = (fun (_, _, _, size , _, _, _, _, _)  -> size)
let byBidSize   = (fun (_, bid  , _, _, _, _, _, _, _)   -> bid)
let byask   = (fun (_, _, ask  , _, _, _, _, _, _)   -> ask)

let sortAndGroup indexFunc data = 
        data
            |> Seq.sortBy indexFunc
            |> Seq.groupBy indexFunc
            |> Seq.toArray

let summaryStats dataGroup data = 

        data
        |> Seq.fold (fun (sizes:Map<int,(float*int)>) x -> 
            let index, xs = x 
            let timeCnt = if sizes.ContainsKey index then sizes.[index] else (0., 0)
            let sums = 
                   xs 
                   |> Seq.map (fun (_, _, _, _, _, _, _, span, cnt) -> (span, cnt))
                   |> Seq.fold (fun (tlSpan, tlCnt) (span, cnt) -> (tlSpan + span), (tlCnt + cnt) ) timeCnt

            sizes.Add(index, sums) 

            ) dataGroup 


let summaryStatsbyDepth mapByDepth dataByDepth  = 

        dataByDepth
        |> Seq.fold(fun (mapByDepth:Map<int,(Map<int,(float*int)>)>) (depth, xs) -> 
                let bySize = xs |> sortAndGroup bySize
                let mapBySize = mapByDepth.[depth]
                mapByDepth.Add (depth, (summaryStats mapBySize bySize))
                ) mapByDepth


let processFiles directory =

        let stats = 
            [0..5]
            |> Seq.fold (fun (acc:Map<int,(Map<int,(float*int)>)>) x -> 
                   acc.Add(x, Map.empty<int,(float*int)>)) Map.empty<int,(Map<int,(float*int)>)>

        (Directory.EnumerateFiles(directory) |> Seq.toList |> List.tail |> List.toSeq)
        |> Seq.map (fun x -> 
            printfn "Parsing file: %s" (Path.GetFileNameWithoutExtension(x))
            parseFile x |> sortAndGroup byDepth )
        |> Seq.fold (fun acc x -> summaryStatsbyDepth acc x ) stats




let flatten (r:Map<int,(Map<int,(float*int)>)>) = seq {
        for k in r do
            let depth = k.Key
            let m = k.Value
            for kk in m do
                let size = kk.Key
                let (tm,cnt) = kk.Value
                yield (depth, size, tm, cnt) }
                 
           
let results = processFiles dataPath |> flatten |> Seq.toArray

results |> Seq.toArray

let file = 
        results 
        |> Seq.map (fun (depth, size, tm, cnt) -> sprintf "%i,%i,%.2f,%i" depth size tm cnt)
        |> Seq.toArray

let output = Path.Combine(outputPath, "kse-kospi200_2013-12_20131021_27270_Stats.csv")

File.WriteAllLines(output, file)

//do  results.[1] |> Map.iter (fun k (tm, cnt) -> printfn "Size: %i \tms: %.0f \tcnt: %i" k tm cnt)


