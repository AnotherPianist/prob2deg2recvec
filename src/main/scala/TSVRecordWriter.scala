import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.hadoop.mapred.{RecordWriter, Reporter}

import java.io.DataOutputStream

class TSVRecordWriter(out: DataOutputStream) extends RecordWriter[Long, LongOpenHashBigSet] {
  @inline final val utf8 = "UTF-8"

  @inline final val newline = "\n".getBytes(utf8)
  @inline final val keyValueSeparator = "\t".getBytes(utf8)

  @inline override def write(key: Long, value: LongOpenHashBigSet): Unit = {
    val iteration = value.iterator()
    val k = key.toString.getBytes(utf8)
    while (iteration.hasNext) {
      val v = iteration.nextLong().toString.getBytes(utf8)
      synchronized {
        out.write(k)
        out.write(keyValueSeparator)
        out.write(v)
        out.write(newline)
      }
    }
  }

  override def close(reporter: Reporter): Unit = synchronized {
    out.close()
  }
}