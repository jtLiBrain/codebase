package priv.jasonLBase.scala.scalaj

import scalaj.http.{Http, HttpResponse}


object Main {
  def main(args: Array[String]): Unit = {
    val response: HttpResponse[String] = Http("http://www.baidu.com").asString

    println(response.body)
    println(response.code)
    println(response.headers)
    println(response.contentType)

  }
}
