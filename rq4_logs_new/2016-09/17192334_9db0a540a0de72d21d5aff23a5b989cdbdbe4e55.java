/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) 2013-2016, Kenneth Leung. All rights reserved. */

package czlab.skaro.server;

import java.lang.reflect.Method;
import java.io.File;

/**
 * @author Kenneth Leung
 */
public class Runtime {

  /**/
  public static void main(String[] args) {

    if (args.length < 1) {
      System.out.println("usage: Runtime <skaro-home>");
    }
    else
    try {
      File cwd= new File(System.getProperties().getProperty("user.dir"));
      File home= new File(args[0]);
      ClassLoader cl= CljAppLoader.newInstance(home, cwd);
      //Thread.currentThread().setContextClassLoader(cl);
      Class<?> z= cl.loadClass("czlab.skaro.server.Cljshim");
      Method m= z.getDeclaredMethod("newrt",ClassLoader.class, String.class);
      Object obj= m.invoke(null, cl, "skaro-runner");
      m=z.getDeclaredMethod("callEx", String.class, Object[].class);
      m.invoke(obj, "czlab.skaro.etc.shell/-main", (Object[])args);
    }
    catch (Throwable t) {
      t.printStackTrace();
    }
    finally {
    }
  }

}

