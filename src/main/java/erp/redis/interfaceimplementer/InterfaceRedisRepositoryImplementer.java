package erp.redis.interfaceimplementer;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.lang.reflect.*;
import java.util.HashMap;
import java.util.Map;

public class InterfaceRedisRepositoryImplementer {

    private static Map<String, Object> itfTypeInstanceMap = new HashMap<>();

    public static synchronized <I> I instance(Class<I> itfType, RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        if (itfTypeInstanceMap.containsKey(itfType.getName())) {
            return (I) itfTypeInstanceMap.get(itfType.getName());
        }
        String newTypeClsName = defineClass(itfType);
        Constructor constructor = null;
        try {
            constructor = Class.forName(newTypeClsName).getDeclaredConstructor(RedisTemplate.class, RedissonClient.class);
        } catch (Exception e) {
            throw new RuntimeException("getDeclaredConstructor for " + newTypeClsName + " error", e);
        }
        constructor.setAccessible(true);
        try {
            I instance = (I) constructor.newInstance(redisTemplate, redissonClient);
            itfTypeInstanceMap.put(itfType.getName(), instance);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("newInstance for " + newTypeClsName + " error", e);
        }
    }

    public static synchronized <I> I instance(Class<I> itfType, Class entityType, Class idType, RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient) {
        if (itfTypeInstanceMap.containsKey(itfType.getName())) {
            return (I) itfTypeInstanceMap.get(itfType.getName());
        }
        String newTypeClsName = defineClass(itfType, entityType, idType);
        Constructor constructor = null;
        try {
            constructor = Class.forName(newTypeClsName).getDeclaredConstructor(RedisTemplate.class, RedissonClient.class);
        } catch (Exception e) {
            throw new RuntimeException("getDeclaredConstructor for " + newTypeClsName + " error", e);
        }
        constructor.setAccessible(true);
        try {
            I instance = (I) constructor.newInstance(redisTemplate, redissonClient);
            itfTypeInstanceMap.put(itfType.getName(), instance);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("newInstance for " + newTypeClsName + " error", e);
        }
    }

    public static synchronized <I> I instance(Class<I> itfType, RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        if (itfTypeInstanceMap.containsKey(itfType.getName())) {
            return (I) itfTypeInstanceMap.get(itfType.getName());
        }
        String newTypeClsName = defineClass(itfType);
        Constructor constructor = null;
        try {
            constructor = Class.forName(newTypeClsName).getDeclaredConstructor(RedisTemplate.class, RedissonClient.class, long.class);
        } catch (Exception e) {
            throw new RuntimeException("getDeclaredConstructor for " + newTypeClsName + " error", e);
        }
        constructor.setAccessible(true);
        try {
            I instance = (I) constructor.newInstance(redisTemplate, redissonClient, maxLockTime);
            itfTypeInstanceMap.put(itfType.getName(), instance);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("newInstance for " + newTypeClsName + " error", e);
        }
    }

    public static synchronized <I> I instance(Class<I> itfType, Class entityType, Class idType, RedisTemplate<String, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
        if (itfTypeInstanceMap.containsKey(itfType.getName())) {
            return (I) itfTypeInstanceMap.get(itfType.getName());
        }
        String newTypeClsName = defineClass(itfType, entityType, idType);
        Constructor constructor = null;
        try {
            constructor = Class.forName(newTypeClsName).getDeclaredConstructor(RedisTemplate.class, RedissonClient.class, long.class);
        } catch (Exception e) {
            throw new RuntimeException("getDeclaredConstructor for " + newTypeClsName + " error", e);
        }
        constructor.setAccessible(true);
        try {
            I instance = (I) constructor.newInstance(redisTemplate, redissonClient, maxLockTime);
            itfTypeInstanceMap.put(itfType.getName(), instance);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("newInstance for " + newTypeClsName + " error", e);
        }
    }

    private static <I> String defineClass(Class<I> itfType, Class entityType, Class idType) {
        byte[] newClsBytes = new byte[0];
        TypeVariable<Class<I>>[] typeVariables = itfType.getTypeParameters();
        if (typeVariables.length > 0) {
            Type[] entityTypeBounds = typeVariables[0].getBounds();
            Class bridgeEntityType = (Class) entityTypeBounds[0];
            String templateBridgeEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntity;";
            newClsBytes = generateNewClsBytes(itfType, entityType, bridgeEntityType, templateBridgeEntityTypeDesc, Object.class, "erp/redis/interfaceimplementer/GenericTemplateEntityRepositoryImpl.class");
        } else {
            String templateBridgeEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntityImpl;";
            newClsBytes = generateNewClsBytes(itfType, entityType, entityType, templateBridgeEntityTypeDesc, idType, "erp/redis/interfaceimplementer/TemplateEntityRepositoryImpl.class");
        }
        String newTypeClsName = "erp.redis.repository.generated." + itfType.getName();
        callClDefineClass(newTypeClsName, newClsBytes);
        return newTypeClsName;
    }

    private static <I> String defineClass(Class<I> itfType) {
        byte[] newClsBytes = new byte[0];
        TypeVariable<Class<I>>[] typeVariables = itfType.getTypeParameters();
        if (typeVariables.length > 0) {
            //接口类型是泛型，自己无法得知具体类型，需从外面传进来，调用方法错误
            throw new RuntimeException("itfType has type parameter(s) , don't know real type here. try to use 'instance' method that with entityType and idType parameters.");
        }
        //接口类型不是泛型，那么先从它继承泛型接口时确定的类型寻找，如果找不到那就是 Object ，如果就没有继承接口那么解析 find 方法获得
        Class entityType = null;
        Class idType = null;
        if (itfType.getInterfaces().length > 0) {
            java.lang.reflect.Type genType = itfType.getGenericInterfaces()[0];
            while (true) {
                if (genType instanceof ParameterizedType) {
                    break;
                }
                if (((Class) genType).getInterfaces().length == 0) {
                    genType = null;
                    break;
                }
                genType = ((Class) genType).getGenericInterfaces()[0];
            }
            if (genType == null) {
                entityType = Object.class;
                idType = Object.class;
            } else {
                java.lang.reflect.Type[] paramsTypes = ((ParameterizedType) genType).getActualTypeArguments();
                entityType = (Class) paramsTypes[0];
                if (paramsTypes.length == 1) {
                    while (true) {
                        if (genType instanceof ParameterizedType) {
                            if (((ParameterizedType) genType).getActualTypeArguments().length >= 2) {
                                break;
                            }
                        }
                        if (((Class) ((ParameterizedType) genType).getRawType()).getInterfaces().length == 0) {
                            genType = null;
                            break;
                        }
                        genType = ((Class) ((ParameterizedType) genType).getRawType()).getGenericInterfaces()[0];
                    }
                    if (genType == null) {
                        idType = Object.class;
                    }
                    paramsTypes = ((ParameterizedType) genType).getActualTypeArguments();
                    idType = (Class) paramsTypes[1];
                } else {
                    idType = (Class) paramsTypes[1];
                }
            }
        } else {
            for (Method method : itfType.getMethods()) {
                if (method.getName().equals("find")) {
                    entityType = method.getReturnType();
                    idType = method.getParameterTypes()[0];
                    break;
                }
            }
        }
        String templateBridgeEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntityImpl;";
        newClsBytes = generateNewClsBytes(itfType, entityType, entityType, templateBridgeEntityTypeDesc, idType, "erp/redis/interfaceimplementer/TemplateEntityRepositoryImpl.class");
        String newTypeClsName = "erp.redis.repository.generated." + itfType.getName();
        callClDefineClass(newTypeClsName, newClsBytes);
        return newTypeClsName;
    }

    private static void callClDefineClass(String newTypeClsName, byte[] newClsBytes) {
        Object[] argArray = new Object[]{newTypeClsName, newClsBytes, Integer.valueOf(0), Integer.valueOf(newClsBytes.length)};
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class cls = null;
        try {
            cls = Class.forName("java.lang.ClassLoader");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("get class for java.lang.ClassLoader error", e);
        }
        Method method = null;
        try {
            method = cls.getDeclaredMethod(
                    "defineClass",
                    new Class[]{String.class, byte[].class, int.class, int.class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("get getDeclaredMethod for defineClass error", e);
        }
        method.setAccessible(true);
        try {
            method.invoke(cl, argArray);
        } catch (Exception e) {
            throw new RuntimeException("invoke defineClass error", e);
        }
    }

    private static byte[] generateNewClsBytes(Class itfType, Class entityType, Class bridgeEntityType, String templateBridgeEntityTypeDesc, Class bridgeIdType, String tplClassResourceName) {
        String entityTypeDesc = "L" + entityType.getName().replace('.', '/') + ";";
        String bridgeEntityTypeDesc = "L" + bridgeEntityType.getName().replace('.', '/') + ";";
        String templateEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntityImpl;";

        String idTypeDesc = "L" + bridgeIdType.getName().replace('.', '/') + ";";
        String templateIdTypeDesc = "Ljava/lang/Object;";

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(tplClassResourceName);
        byte[] bytes = new byte[0];
        try {
            bytes = new byte[is.available()];
            is.read(bytes);
        } catch (IOException e) {
            throw new RuntimeException("read TemplateEntityRepositoryImpl.class error", e);
        }

        String newTypeClsName = "erp.redis.repository.generated." + itfType.getName();
        ClassReader cr = new ClassReader(bytes);
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
            @Override
            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
                interfaces[0] = itfType.getName().replace('.', '/');
                name = newTypeClsName.replace('.', '/');
                signature = signature.replaceAll(templateEntityTypeDesc, entityTypeDesc);
                super.visit(version, access, name, signature, superName, interfaces);
            }

            @Override
            public MethodVisitor visitMethod(int access, String mthName, String mthDesc, String signature, String[] exceptions) {
                mthDesc = mthDesc.replaceAll(templateBridgeEntityTypeDesc, bridgeEntityTypeDesc).replaceAll(templateIdTypeDesc, idTypeDesc);
                return new AdviceAdapter(Opcodes.ASM5, super.visitMethod(access, mthName, mthDesc, signature, exceptions), access, mthName, mthDesc) {
                    @Override
                    public void visitTypeInsn(final int opcode, final String type) {
                        String realType = type;
                        if (Opcodes.CHECKCAST == opcode) {
                            realType = bridgeEntityType.getName().replace('.', '/');
                        }
                        super.visitTypeInsn(opcode, realType);
                    }
                };
            }
        }, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }

}
