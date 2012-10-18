package com.usergrid.count;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.usergrid.count.common.Count;

public class AsyncSimpleBatcher extends AbstractBatcher {
	
	private Logger log = LoggerFactory.getLogger(AsyncSimpleBatcher.class);
	private int batchSize = 500;
	private AtomicLong batchSubmissionCount = new AtomicLong();
	
	// if last batch time elpased more than this, execute batch ignoring batchSize
	private long MAX_INTERVAL = 100;
	private long lastBatchTime = System.currentTimeMillis();
	
	private static ExecutorService executor = Executors.newSingleThreadExecutor();
	
	public AsyncSimpleBatcher(int queueSize) {
		super(queueSize);
	}

	
	protected synchronized boolean maybeSubmit(Batch batch) {

		if(batch.getPayloadSize()==0) { return false; }
		
		long elapsed = System.currentTimeMillis() - lastBatchTime;
		if(elapsed<MAX_INTERVAL && batch.getLocalCallCount()<batchSize) {
			return false;
		}
		lastBatchTime = System.currentTimeMillis();
		
		Collection<Count> counts = batch.getCounts();
		Batch cloneBatch = new Batch();
		for(Count count : counts) {
			Count cloneCount = new Count(count.getTableName(), count.getKeyName(), count.getColumnName(), count.getValue());
			cloneBatch.add(cloneCount);
		}
		
		log.debug("submit triggered...");
		executor.execute(new BatchRunner(cloneBatch));
		
		return true;

	}

	private class BatchRunner implements Runnable {

		private Batch batch = null;
		
		private BatchRunner(Batch batch) {
			this.batch = batch;
		}
		
		
		@Override
		public void run() {
			if(batch==null) { return; }
			log.debug("start batching. batch count={}, call count={}", batch.getPayloadSize(), batch.getLocalCallCount());
			try {
				Future f = batchSubmitter.submit(batch);
				f.get();
				batchSubmissionCount.incrementAndGet();
			} catch (Exception e) {
				log.warn("batching failed.", e);
			} finally {
				log.debug("finished batching. batch count={}, call count={}", batch.getPayloadSize(), batch.getLocalCallCount());
			}
		}
	
	}
	
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public long getBatchSubmissionCount() {
		return batchSubmissionCount.get();
	}


}
