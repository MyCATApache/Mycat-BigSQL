package org.opencloudb.sqlcmd;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.net.FrontSession;

/**
 * sql command like set xxxx ,only return OK /Err Pacakage,can't return restult
 * set
 * 
 * @author wuzhih
 * 
 */
public interface SQLCtrlCommand {

	boolean isAutoClearSessionCons();
	boolean releaseConOnErr();
	
	boolean relaseConOnOK();
	
	void sendCommand(FrontSession session, BackendConnection con);

	/**
	 * 收到错误数据包的响应处理
	 */
	void errorResponse(FrontSession session,byte[] err,int total,int failed);

	/**
	 * 收到OK数据包的响应处理
	 */
	void okResponse(FrontSession session, byte[] ok);

}
