package org.opencloudb.sqlengine;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.net.FrontSession;
import org.opencloudb.net.mysql.EOFPacket;

public class EngineCtx {
	public static final Logger LOGGER = Logger.getLogger(EngineCtx.class);
	private final BatchSQLJob bachJob;
	private AtomicInteger jobId = new AtomicInteger(0);
	AtomicInteger packetId = new AtomicInteger(0);
	private final FrontSession session;
    private AtomicBoolean finished=new AtomicBoolean(false);
	public EngineCtx(FrontSession session) {
		this.bachJob = new BatchSQLJob();
		this.session = session;
	}

	public byte incPackageId() {
		return (byte) packetId.incrementAndGet();
	}

	public void executeNativeSQLSequnceJob(String[] dataNodes, String sql,
			SQLJobHandler jobHandler) {
		for (String dataNode : dataNodes) {
			SQLJob job = new SQLJob(jobId.incrementAndGet(), sql, dataNode,
					jobHandler, this);
			bachJob.addJob(job, false);

		}
	}

	public void executeNativeSQLParallJob(String[] dataNodes, String sql,
			SQLJobHandler jobHandler) {
		for (String dataNode : dataNodes) {
			SQLJob job = new SQLJob(jobId.incrementAndGet(), sql, dataNode,
					jobHandler, this);
			bachJob.addJob(job, true);

		}
	}

	/**
	 * set no more jobs created
	 */
	public void endJobInput()
	{
		bachJob.setNoMoreJobInput(true);
	}
	public void writeEof() {
		ByteBuf buf=session.allocate(64);
		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = incPackageId();
		eofPckg.write(buf);
		session.write(buf);
	}

	public FrontSession getSession() {
		return session;
	}

	public void onJobFinished(SQLJob sqlJob) {

		boolean allFinished = bachJob.jobFinished(sqlJob);
		if (allFinished && finished.compareAndSet(false, true)) {
			LOGGER.info("all job finished  for front connection: "
					+ session.getConInfo());
			writeEof();
		}

	}
}
