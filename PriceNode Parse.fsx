#I "packages/FSharp.Data.2.0.5"
#r "lib/net40/FSharp.Data.dll"
#r "packages/FSharpx.Core.1.8.41/lib/40/FSharpx.Core.dll"

open System
open System.IO 
open System.Collections.Generic
open FSharp.Data
open System.Threading
open FSharp.Control


    // @ y

let srcDir = __SOURCE_DIRECTORY__

let dataPath = Path.Combine(srcDir,   "../futures/csv by contract/Front month/")
let outputPath = Path.Combine(srcDir, "../futures/csv by contract/Lazy Read Test/output/")

//let testDataPath = Path.Combine(dataPath,"kse-kospi200_2013-12_20131021.csv")
let testDataPath = Path.Combine(dataPath,"kse-kospi200_2013-12_20130916.csv")
let testDataPathLarge = Path.Combine(dataPath,"kse-kospi200_2013-12_20131212.csv")

[<CustomEquality; CustomComparison>]
type PriceNode = 
    { time:DateTime; price:float; bid:int; ask:int; lastSize:int; depth:int}
    override x.ToString () =
        sprintf "%s,%.2f,%d,%d,%d,%d" (x.time.ToUniversalTime().ToString("O")) x.price x.bid x.ask x.lastSize x.depth

    member x.ToStringNoDate () =
        sprintf "%.2f,%d,%d,%d,%d" x.price x.bid x.ask x.lastSize x.depth

    override x.Equals(yobj) =
        match yobj with
        | :? PriceNode as y -> (x.price = y.price) &&  (x.bid = y.bid) &&  (x.ask = y.ask) &&  (x.lastSize = y.lastSize) && (x.depth = y.depth)
        | _ -> false

    override x.GetHashCode() = hash x.price
    interface System.IComparable with
      member x.CompareTo yobj =
          match yobj with
          | :? PriceNode as y -> compare x.price y.price
          | _ -> invalidArg "yobj" "cannot compare values of different types"


type Trade     = { time:DateTime; price:float; size:int; }

[<CustomEquality; CustomComparison>]
type Quote =
    { time:DateTime; price:float; size:int; depth:int }

    override x.Equals(yobj) =
        match yobj with
        | :? Quote as y -> (x.price = y.price) &&  (x.size = y.size) && (x.depth = y.depth)
        | _ -> false

    override x.GetHashCode() = hash x.price
    interface System.IComparable with
      member x.CompareTo yobj =
          match yobj with
          | :? Quote as y -> compare x.price y.price
          | _ -> invalidArg "yobj" "cannot compare values of different types"

type Tick = 
    | Bid   of Quote
    | Ask   of Quote
    | Empty of Quote
    | Trade of Trade

    member x.price () = 
         match x with
         | Bid x | Ask x | Empty x -> x.price
         | Trade x -> x.price

    member x.size () = 
         match x with
         | Bid x | Ask x | Empty x -> x.size
         | Trade x -> x.size

    member x.depth () = 
         match x with
         | Bid x | Ask x | Empty x -> x.depth
         | Trade x -> 0

    member x.time () = 
         match x with
         | Bid x | Ask x | Empty x -> x.time
         | Trade x -> x.time

    override x.ToString () =
        match x with
        | Bid x   -> sprintf " %s Bid-%i %i @ %.0f"    (x.time.ToUniversalTime().ToString("o")) x.depth x.size x.price
        | Ask x   -> sprintf " %s Ask-%i %i @ %.0f"    (x.time.ToUniversalTime().ToString("o")) x.depth x.size x.price
        | Empty x -> sprintf " %s Empty-%i %i @ %.0f"  (x.time.ToUniversalTime().ToString("o")) x.depth x.size x.price
        | Trade x -> sprintf " %s Trade %i @ %.0f"     (x.time.ToUniversalTime().ToString("o")) x.size         x.price  


type PriceEvent =
    | Ticks  of seq<Tick>
    | Completed

let toTickSeq (x:string[]) (tickSize:float) =
    let event = x.[1]
    match event, (x:string[]) with
    | "quote", x -> seq {
        let date = DateTime.Parse(x.[0])

        yield Ask {time = date; price = Double.Parse(x.[24]); size = Int32.Parse(x.[23]); depth = 5}
        yield Ask {time = date; price = Double.Parse(x.[22]); size = Int32.Parse(x.[21]); depth = 4}
        yield Ask {time = date; price = Double.Parse(x.[20]); size = Int32.Parse(x.[19]); depth = 3} 
        yield Ask {time = date; price = Double.Parse(x.[18]); size = Int32.Parse(x.[17]); depth = 2}
        let bestBid = Double.Parse(x.[6])
        let bestAsk = Double.Parse(x.[16])
        let spread = bestAsk - bestBid
        yield Ask {time = date; price = bestAsk;              size = Int32.Parse(x.[15]); depth = 1}

        if spread > tickSize then // add it empty price node messages
            for price in (bestBid + tickSize) .. tickSize .. (bestAsk - tickSize) do
                //printfn "Empty {price = %.0f;  size = 0;  depth = 0} start = %.0f, End = %.0f " price (bestBid) (bestAsk)
                yield Empty {time = date; price = price;  size = 0;  depth = 0}
            
        yield Bid {time = date; price = bestBid;              size = Int32.Parse(x.[5]);  depth = 1}
        yield Bid {time = date; price = Double.Parse(x.[8]);  size = Int32.Parse(x.[7]);  depth = 2}
        yield Bid {time = date; price = Double.Parse(x.[10]); size = Int32.Parse(x.[9]);  depth = 3}
        yield Bid {time = date; price = Double.Parse(x.[12]); size = Int32.Parse(x.[11]); depth = 4}
        yield Bid {time = date; price = Double.Parse(x.[14]); size = Int32.Parse(x.[13]); depth = 5} }
    | _ , x ->      seq { 
        yield Trade {time = DateTime.Parse(x.[0]); price = Double.Parse(x.[4]) ; size = Int32.Parse(x.[3]);} }
    

let OutPutPriceNodes dataFile outputPath (price:float, (pNodes:ResizeArray<DateTime * PriceNode[]>)) = //async { 
      
        let outputFile = 
            let sourcefile = Path.GetFileNameWithoutExtension dataFile
            let tokens = sourcefile.Split(char("_")) 
            let outputFolder = Path.Combine(outputPath,(sprintf "%s/" tokens.[2])) 
            let outputFile = Path.Combine(outputFolder,(sprintf "%s_%.0f.csv" sourcefile price))
            printfn "Preparing %s file" (sprintf "%.0f/%s_%.0f.csv" price sourcefile price)
            outputFile 
              
        let stpWatch = new System.Diagnostics.Stopwatch()
        stpWatch.Start()

        let rows =
                pNodes
                |> Seq.pairwise
                |> Seq.filter(fun (a,b) ->  ((snd a) <> (snd b)))
                |> Seq.map(fun (a,b) -> b)
                |> Seq.append ([| pNodes |> Seq.head |])
                |> Seq.map(fun ((time:DateTime), (prices:PriceNode[]))  -> 

                            ( // format latest state as a string
                            let timeStamp = String.Concat(time.ToUniversalTime().ToString("o"),",")    
                            let pNodesAsStr = prices |> Seq.map(fun pn -> pn.ToStringNoDate())
                            String.Concat(timeStamp, (String.concat "," pNodesAsStr))))
                |> Seq.toArray

        (new FileInfo(outputFile)).Directory.Create()

        printfn "Writing %s file" (sprintf "%.0f/%s_%.0f.csv" price (Path.GetFileNameWithoutExtension testDataPath) price)
            
        File.WriteAllLines(outputFile, rows)

        let rows = []

        stpWatch.Stop()
        printfn  "Async @ %.0f completed in %s ms  with %d rows of %d rows" price  (stpWatch.Elapsed.ToString())  rows.Length pNodes.Count
         
        pNodes.Clear()

        //}


type Stream (filePath:string, outputPath, tickSize, tickSpan) = 
    
    let updatedPrices (prices:SortedDictionary<float,PriceNode>) (ticks:array<Tick>) = 
            ticks
            |> Array.iter (fun tick -> 
                let price = tick.price()
                let hasPrcNode = prices.ContainsKey(price)
                match hasPrcNode, tick with 
                // price node already exists
                | true , Trade t -> prices.[price] <- { prices.[price] with time = t.time; lastSize = t.size;}
                | true , Bid q   -> prices.[price] <- { prices.[price] with time = q.time; bid = q.size; ask = 0;      lastSize = 0; depth = q.depth}
                | true , Ask q   -> prices.[price] <- { prices.[price] with time = q.time; bid = 0;      ask = q.size; lastSize = 0; depth = q.depth}
                | true , Empty q -> prices.[price] <- { prices.[price] with time = q.time; bid = 0;      ask = 0;      lastSize = 0; depth = 0}
                // new price node
                | false, Trade t -> prices.Add(price, { time = t.time; price = t.price; bid = 0;      ask = 0;      lastSize = t.size; depth = 0})     
                | false, Bid q   -> prices.Add(price, { time = q.time; price = q.price; bid = q.size; ask = 0;      lastSize = 0;      depth = q.depth})
                | false, Ask q   -> prices.Add(price, { time = q.time; price = q.price; bid = 0;      ask = q.size; lastSize = 0;      depth = q.depth})
                | false, Empty q -> prices.Add(price, { time = q.time; price = q.price; bid = 0;      ask = 0;      lastSize = 0;      depth = 0})  )       

    member x.workflow = 
        async {

            let prices = new SortedDictionary<float,PriceNode>()
            let priceNodeHist = new SortedDictionary<float,ResizeArray<DateTime * PriceNode[]>>()
            let lastTick = new Dictionary<float,Tick>()
            use sr = new StreamReader ((filePath:string))

            let firstLine =  sr.ReadLine ()

            printfn "Starting Processing..."

            let sw = new System.Diagnostics.Stopwatch()
            sw.Start()
            //printfn "%s" firstLine
            let cnt = ref 0

//            while !cnt <= 50 do
            while not sr.EndOfStream do
                let line =  sr.ReadLine ()
                let row = line.Split(char(","))   

                let ticks = toTickSeq row tickSize |> Seq.toArray
                let tickTime = ticks.[0].time()

                let ticksNoZeros = ticks |> Array.filter (fun tick -> 
                        if tick.price() = 0. then false else true)

                let updatedPriceNodes = updatedPrices prices ticksNoZeros 
                    
                let pNodes = prices.Values // |> Seq.toArray 

                let pNodesOfN = pNodes |> Seq.windowed 5 

                let pNodesSeq = pNodesOfN |> Seq.map(fun x ->
                                    let middle = x.[2]
                                    (middle.price, tickTime, x))

                pNodesSeq |> Seq.iter(fun (p, tm, pns) -> 
                                if not (priceNodeHist.ContainsKey(p)) then
                                     priceNodeHist.Add(p, (new ResizeArray<DateTime * PriceNode[]> ([(tm, pns)])))
                                else priceNodeHist.[p].Add(tm, pns))
               
                cnt:= !cnt + 1

            //sw.Stop()
//            newTick.Trigger Completed
            printfn "Completed parse in %s ms" (sw.Elapsed.ToString())
            printfn "Writing %d Price Nodes..." (priceNodeHist.Values |> Seq.length)

            for price in priceNodeHist.Keys do
                //if price > 26445. then
                    OutPutPriceNodes filePath outputPath (price, priceNodeHist.[price])

            sw.Stop()

            printfn "Completed all in %s ms" (sw.Elapsed.ToString())
            priceNodeHist.Clear()

            }


let myStream  = new Stream (testDataPath, outputPath, 5., 2)



let cts =  new CancellationTokenSource()
try Async.RunSynchronously(myStream.workflow, cancellationToken = cts.Token)
with :? OperationCanceledException -> () 


cts.Dispose()
