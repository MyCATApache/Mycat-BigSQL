package org.opencloudb.sqlengine;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.FrontSession;

public class MutilNodeCoordListener {

	public void failed(String errMsg, int totalNodes, int failedNodes,
			FrontSession session) {
		session.writeErrMessage(ErrorCode.ERR_MULTI_NODE_FAILED, "total "
				+ totalNodes + ",failed " + failedNodes + " " + errMsg);

	}

	public void finished(FrontSession session) {

	}
}
