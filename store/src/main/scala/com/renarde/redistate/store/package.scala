package com.renarde.redistate

package object store {

    case class RedisConf(host: String, port: Int, prefix: String) {
        val bytePrefix: Array[Byte] = s"$prefix:".getBytes
    }

}
