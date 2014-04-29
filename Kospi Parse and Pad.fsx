
open System
open System.IO

let trim (s:string) = s.Trim

let srcDir = __SOURCE_DIRECTORY__
let dataPath = Path.Combine(srcDir, "../futures/text version/") 
let outputPath = Path.Combine(srcDir, "../futures/csv by contract/") 

let files  = System.IO.Directory.GetFiles(dataPath) |> Array.toList |> List.tail
let testDataFile = files.Head

let colNames = ["Date"; "event"; "contract"; "lastSize"; "last";
                "bidSz"; "bid"; "bidSz1"; "bid1"; "bidSz2"; "bid2"; "bidSz3"; "bid3"; "bidSz4"; "bid4";
                "askSz"; "ask"; "askSz1"; "ask1"; "askSz2"; "ask2"; "askSz3"; "ask3"; "askSz4"; "ask4";]

colNames.Length
let header = String.concat "," colNames 


let GetSortedPaddedSeq (items:seq<string>) =
    use e = items.GetEnumerator()
    let chars = [|','; '@'|]
    let rec loop (sorted:Map<string,List<string>>) =
        if e.MoveNext() then
            let line = e.Current
            let e = line.Split chars
            let contract = e.[2]
            let full = 
                match e.[1] with
                | "trade" -> Array.toList e @ [ for i in 1 .. 20 -> "0" ] |> List.toArray
                | _ ->  let hd = e.[0..2]  |> Array.toList
                        let tl = e.[3..22] |> Array.toList
                        let final = hd @ [ "0"; "0" ] @ tl
                        (final |> List.toArray)
            
            let commaSeparated = String.concat "," full 
            let contractData  = if sorted.ContainsKey contract then sorted.[contract] else List.empty<string>
            let sorted = sorted.Add (contract, commaSeparated :: contractData)
            loop sorted
        else sorted
    loop Map.empty<string,List<string>>

let writeout date (contract:string) data = 

    let componets = contract.Split ' ' |> Seq.toList
    let csvFileName = sprintf "%s_%s_%s.csv" componets.[1] componets.[2] date
    let filePath = Path.Combine (outputPath, csvFileName)

    // reverse data list (old to newest) and output to array
    let outData = data |> List.rev |> List.toArray
    let withHeaders = Array.append [|header|] outData

    // writeout the data
    File.WriteAllLines(filePath , withHeaders) 


let processFile fileName = 

    // parse file name for date
    let fName = Path.GetFileNameWithoutExtension fileName
    let fileElem = fName.Split (char "-")
    let date = fileElem.[1]

    // read in, parse, padded and sort into map of key = contract, value = data
    let file = File.ReadAllLines(fileName)
    let sortedPadded = GetSortedPaddedSeq file

    // writeout each contract
    sortedPadded |> Map.iter (fun contract data -> writeout date contract data)

let processTextFiles fs = 
        fs
        |> List.iter (fun file ->
            printfn "Processing %s ..." (Path.GetFileName(file))
            processFile file |> ignore
            )

do processTextFiles files |> ignore