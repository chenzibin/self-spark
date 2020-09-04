package self.spark.java.demo;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;

/**
 * MyRDD
 *
 * @author chenzb
 * @date 2020/8/15
 */
public class MyRDD extends RDD {

	public MyRDD(SparkContext _sc, Seq<Dependency<?>> deps, ClassTag evidence$1) {
		super(_sc, deps, evidence$1);
	}

	@Override
	public Iterator compute(Partition split, TaskContext context) {
		return null;
	}

	@Override
	public Partition[] getPartitions() {
		return new Partition[0];
	}
}
