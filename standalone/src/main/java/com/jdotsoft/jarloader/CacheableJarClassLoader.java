package com.jdotsoft.jarloader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * store class finding result and don't search again if once failed.
 */
public class CacheableJarClassLoader extends JarClassLoader {

	// store found class by class name
	private static Map<String, Class<?>> loadedClassCache = new HashMap<String, Class<?>>();
	
	// store non existing class name
	private static Set<String> notExistingClassNameSet = new HashSet<String>();

	@Override
	protected synchronized Class<?> loadClass(String sClassName, boolean bResolve) throws ClassNotFoundException {

		if(notExistingClassNameSet.contains(sClassName)) { 
			throw new ClassNotFoundException("Failure to load: " + sClassName);
		}

		Class<?> c = null;
		c = loadedClassCache.get(sClassName);
		if(c!=null) { return c; }
		
		try {
			c = super.loadClass(sClassName, bResolve);
			loadedClassCache.put(sClassName, c);
			return c;
		} catch(ClassNotFoundException e) {
			notExistingClassNameSet.add(sClassName);
			throw e;
		}

	} 

}
