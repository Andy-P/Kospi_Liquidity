#I "packages/FSharp.Charting.Gtk.0.90.6"
#I "packages/Deedle.0.9.12"
#load "Deedle.fsx"

open System
open System.IO
open Deedle

let trim (s:string) = s.Trim

let srcDir = __SOURCE_DIRECTORY__
let dataPath = Path.Combine(srcDir, "../futures/text version/") 
let outputPath = Path.Combine(srcDir, "../futures/csv by contract/") 

let files  = System.IO.Directory.GetFiles(dataPath) |> Array.toList |> List.tail
let testDataFile = files.Head

let colNames = ["Date"; "event"; "contract"; "lastSize"; "last";
                "bidSz"; "bid"; "bidSz1"; "bid1"; "bidSz2"; "bid2"; "bidSz3"; "bid3"; "bidSz4"; "bid4";
                "askSz"; "ask"; "askSz1"; "ask1"; "askSz2"; "ask2"; "askSz3"; "ask3"; "askSz4"; "ask4";]

let header = String.concat ", " colNames 

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
            
            let commaSeparated = String.concat ", " full 
            let contractData  = if sorted.ContainsKey contract then sorted.[contract] else List.empty<string>
            let sorted = sorted.Add (contract, commaSeparated :: contractData)
            loop sorted
        else sorted
    loop Map.empty<string,List<string>>

let writeout date (contract:string) (data:List<string>) = 
    let componets = contract.Split ' ' |> Seq.toList
    let fName = sprintf "%s_%s_%s.csv" componets.[1] componets.[2] date
    let csvFilename = Path.Combine (outputPath, fName)
    let outData = data |> List.rev |> List.toArray
    let withHeaders = Array.append [|header|] outData
    File.WriteAllLines(csvFilename , withHeaders) 

let processFile (file:string) = 
    let fName = Path.GetFileNameWithoutExtension file
    let fileElem = fName.Split (char "-")
    let date = fileElem.[1]
    let file = File.ReadAllLines(file)
    let sortedPadded = GetSortedPaddedSeq file
    sortedPadded |> Map.iter (fun (file:string) (data:List<string>) -> writeout date file data)

let processTextFiles fs = 
        fs
        |> List.iter (fun file ->
            printfn "Processing %s ..." (Path.GetFileName(file))
            processFile file |> ignore
            )

do processTextFiles files |> ignore