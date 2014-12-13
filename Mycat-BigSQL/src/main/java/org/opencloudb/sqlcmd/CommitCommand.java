package org.opencloudb.sqlcmd;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.net.FrontSession;
import org.opencloudb.net.mysql.ErrorPacket;

public class CommitCommand implements SQLCtrlCommand {

	@Override
	public void sendCommand(FrontSession session, BackendConnection con) {
		con.commit();
	}

	@Override
	public void errorResponse(FrontSession session, byte[] err,
			int total, int failed) {
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.read(err);
		String errInfo = "total " + total + " failed " + failed + " detail:"
				+ new String(errPkg.message);
		session.setTxInterrupt(errInfo);
		session.writeErrorPkg(errPkg);
	}

	@Override
	public void okResponse(FrontSession session, byte[] ok) {
		session.writeBytes(ok);
	}

	@Override
	public boolean releaseConOnErr() {
		// need rollback when err
		return false;
	}

	@Override
	public boolean relaseConOnOK() {
		return true;
	}

	@Override
	public boolean isAutoClearSessionCons() {
		// need rollback when err
		return false;
	}

}
