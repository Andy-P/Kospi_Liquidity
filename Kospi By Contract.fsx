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

let testDataFile = Path.Combine(dataPath,"kse-kospi200_2013-12_20131021.csv")
//let colNames = ["Date"; "Type"; "Contract"; "LastSize"; "last";
//                "bidSz"; "bid"; "bidSz1"; "bid1"; "bidSz2"; "bid2"; "bidSz3"; "bid3"; "bidSz4"; "bid4";
//                "askSz"; "ask"; "askSz1"; "ask1"; "askSz2"; "ask2"; "askSz3"; "ask3"; "askSz4"; "ask4";]

let mySchema ="Type=string, Contract=string, last=float, bid=float, bid1=float, bid2=float, bid3=float, bid4=float, ask=float, ask1=float, ask2=float, ask3=float, ask4=float"

let KospiUnOrdered = Frame.ReadCsv(testDataFile, hasHeaders = true, schema = mySchema)

let rowCnt = KospiUnOrdered.RowCount

let LstData = KospiUnOrdered.Columns.[ ["Date"; "Type"; "last"; "LastSize"] ]

let trades = LstData.RowsDense 
             |> Series.filterValues (fun row -> row.GetAs<string>("Type").Contains("trade"))
             |> Frame.ofRows
             |> Frame.indexRowsDate "Date"
             |> Frame.orderRows

let byMinute =
      trades
      |> Frame.groupRowsUsing (fun k _ -> DateTime(k.Year, k.Month, k.Day, k.Hour, k.Minute, 1))

let VoumeByMin = byMinute?LastSize  |> Series.sumLevel fst

