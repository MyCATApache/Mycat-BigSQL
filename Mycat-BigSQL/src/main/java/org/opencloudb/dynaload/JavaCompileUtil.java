package org.opencloudb.dynaload;

import java.util.Arrays;
import java.util.Locale;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;

public class JavaCompileUtil {

//
//	public boolean compileJavaSource()
//	{
//		//get system compiler:
//        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
// 
//        // for compilation diagnostic message processing on compilation WARNING/ERROR
//        MyDiagnosticListener c = new MyDiagnosticListener();
//        StandardJavaFileManager fileManager = compiler.getStandardFileManager(c,
//                                                                              Locale.ENGLISH,
//                                                                              null);
//        //specify classes output folder
//        Iterable options = Arrays.asList("-d", classOutputFolder);
//        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager,
//                                                             c, options, null,
//                                                             files);
//        Boolean result = task.call();
//        if (result == true)
//        {
//            System.out.println("Succeeded");
//        }
//	}
//}
//
//class MyDiagnosticListener implements DiagnosticListener<JavaFileObject>
//{
//    public void report(Diagnostic<? extends JavaFileObject> diagnostic)
//    {
//
//        System.out.println("Line Number->" + diagnostic.getLineNumber());
//        System.out.println("code->" + diagnostic.getCode());
//        System.out.println("Message->"
//                           + diagnostic.getMessage(Locale.ENGLISH));
//        System.out.println("Source->" + diagnostic.getSource());
//        System.out.println(" ");
//    }
}
