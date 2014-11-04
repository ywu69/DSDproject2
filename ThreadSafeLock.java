import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadSafeLock {
	private int readers;
	private int writers;

	private static final Logger logger = LogManager.getLogger("DataServer");

	public ThreadSafeLock() {
		this.readers = 0;
		this.writers = 0;
	}

	/*
	 * Will wait until there are no active writers in the system, and then will
	 * increase the number of active readers.
	 */
	public synchronized void lockRead() {
		while (writers > 0) {
			try {
				logger.info("A writer is writing, please wait!");
				this.wait();
			} catch (InterruptedException e) {
				logger.debug(e.getMessage(), e);
			}
		}
		readers++;
	}

	/*
	 * Will decrease the number of active readers, and notify any waiting
	 * threads if necessary.
	 */
	public synchronized void unlockRead() {
		readers--;
		notifyAll();
	}

	/*
	 * Will wait until there are no active readers or writers in the system, and
	 * then will increase the number of active writers.
	 */
	public synchronized void lockWrite() {
		while (readers > 0 || writers > 0) {
			try {
				logger.info("A writer is writing or a reader is reading, please wait!");
				this.wait();
			} catch (InterruptedException e) {
				logger.debug(e.getMessage(), e);
			}
		}
		writers++;
	}

	/*
	 * Will decrease the number of active writers, and notify any waiting
	 * threads if necessary.
	 */
	public synchronized void unlockWrite() {
		writers--;
		notifyAll();
	}
}