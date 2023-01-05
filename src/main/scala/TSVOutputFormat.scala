import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.util.Progressable

import java.io.DataOutputStream

class TSVOutputFormat extends FileOutputFormat[Long, LongOpenHashBigSet] {
  @inline override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable): TSVRecordWriter = {
    val file = FileOutputFormat.getTaskOutputPath(job, name)
    val fs = file.getFileSystem(job)
    val fileOut = fs.create(file, progress)
    getRecordWriter(fileOut)
  }

  @inline final def getRecordWriter(out: DataOutputStream) = new TSVRecordWriter(out)
}