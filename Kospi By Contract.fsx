#I "packages/FSharp.Charting.Gtk.0.90.6"
#I "packages/Deedle.0.9.12"
#load "Deedle.fsx"
#load "FSharp.Charting.Gtk.fsx"

open System
open System.IO
open Deedle
open FSharp.Charting

let srcDir = __SOURCE_DIRECTORY__
let dataPath = Path.Combine(srcDir, "../futures/csv by contract/Front month/")
let outputPath = Path.Combine(srcDir, "../futures/csv by contract/") 
let files  = System.IO.Directory.GetFiles(dataPath) |> Array.toList |> List.tail

// Included for reference
//let colNames = ["Date"; "event"; "contract"; "lastSize"; "last";
//                "bidSz"; "bid"; "bidSz1"; "bid1"; "bidSz2"; "bid2"; "bidSz3"; "bid3"; "bidSz4"; "bid4";
//                "askSz"; "ask"; "askSz1"; "ask1"; "askSz2"; "ask2"; "askSz3"; "ask3"; "askSz4"; "ask4";]

let testDataFile = Path.Combine(dataPath,"kse-kospi200_2013-12_20131021.csv")

let mySchema ="event=string, contract=string, last=float, bid=float, bid1=float, bid2=float, bid3=float, bid4=float, ask=float, ask1=float, ask2=float, ask3=float, ask4=float"

let KospiUnOrdered = Frame.ReadCsv(testDataFile, hasHeaders = true, schema = mySchema)
let rowCnt = KospiUnOrdered.RowCount

let trades = KospiUnOrdered.Columns.[ ["Date"; "event"; "last"; "lastSize"] ].RowsDense
             |> Series.filterValues (fun row -> row.GetAs<string>("event").Contains("trade"))
             |> Frame.ofRows
             |> Frame.indexRowsDate "Date"
             |> Frame.orderRows

let tradesRowCnt = trades.RowCount

let byMinute =
      trades
      |> Frame.groupRowsUsing (fun k _ -> DateTime(k.Year, k.Month, k.Day, k.Hour, k.Minute, 1))


//let byMinuteCnt = byMinute.KeyCount

let VolumeByMin = byMinute?lastSize  |> Series.sumLevel fst

let cnt = VolumeByMin.KeyCount

Chart.Line(VolumeByMin|> Series.observations)
Chart.Column(VolumeByMin.Values)


