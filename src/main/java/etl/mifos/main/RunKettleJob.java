package etl.mifos.main;

import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import etl.mifos.database.ClearDataFromDataBase;

public class RunKettleJob {
    static Logger logger = Logger.getLogger(RunKettleJob.class);

    public static void main(String[] args) {
        try {
            ClearDataFromDataBase clear = new ClearDataFromDataBase();
            clear.resetDatabase(args[2], args[3], args[4]);
            try {
                if (args[1].equals("true")) {
                    KettleEnvironment.init(false);
                } else {
                    KettleEnvironment.init();
                }
                JobMeta jobMeta = new JobMeta(args[0], null);
                Job job = new Job(null, jobMeta);
                job.setName(Thread.currentThread().getName());
                job.setLogLevel(LogLevel.BASIC);
                job.run();
                job.waitUntilFinished();
                if (job.getResult() != null && job.getResult().getNrErrors() != 0) {
                    logger.info("Runnig jobs have some errors");
                }
            } catch (KettleException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
