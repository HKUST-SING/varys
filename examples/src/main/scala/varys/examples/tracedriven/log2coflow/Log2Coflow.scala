/**
 * Main Test
 * Created by wakira on 15-7-15.
 */
package log2coflow

object Log2Coflow {
  def main (args: Array[String]): Unit = {
    val input = scala.io.Source.fromFile(args(0)).getLines()
    val coflow = new YarnMapReduceLogParser(input).run()
    println("Nodes:")
    coflow.nodes.foreach(println)
    println("Flows:")
    coflow.flows.foreach(x => println("S:"+x.source+" D:"+x.dest+" s:"+x.size.toString + " UID:"+x.uid.toString))
  }
}