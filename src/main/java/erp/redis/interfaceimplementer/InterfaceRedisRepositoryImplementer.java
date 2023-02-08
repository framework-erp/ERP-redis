package erp.redis.interfaceimplementer;

import org.objectweb.asm.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

public class InterfaceRedisRepositoryImplementer {

    private static Map<String, Object> itfTypeInstanceMap = new HashMap<>();

    public static synchronized <I> I instance(Class<I> itfType, RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient) {
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

    public static synchronized <I> I instance(Class<I> itfType, RedisTemplate<Object, Object> redisTemplate, RedissonClient redissonClient, long maxLockTime) {
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

    private static <I> String defineClass(Class<I> itfType) {
        byte[] newClsBytes = new byte[0];
        TypeVariable<Class<I>>[] typeVariables = itfType.getTypeParameters();
        if (typeVariables.length > 0) {
            TypeVariable<Class<I>> entityTypeVariable = typeVariables[0];
            Type[] entityTypeBounds = entityTypeVariable.getBounds();
            Class entityType = (Class) entityTypeBounds[0];
            newClsBytes = generateNewClsBytesForGeneric(itfType, entityType);
        } else {
            for (Method method : itfType.getMethods()) {
                if (method.getName().equals("find")) {
                    Class entityType = method.getReturnType();
                    Class idType = method.getParameterTypes()[0];
                    newClsBytes = generateNewClsBytes(itfType, entityType, idType);
                    break;
                }
            }
        }
        String newTypeClsName = "erp.redis.repository.generated." + itfType.getName();
        Object[] argArray = new Object[]{newTypeClsName, newClsBytes,
                new Integer(0), new Integer(newClsBytes.length)};
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
        return newTypeClsName;
    }

    private static byte[] generateNewClsBytes(Class itfType, Class entityType, Class idType) {
        String entityTypeDesc = "L" + entityType.getName().replace('.', '/') + ";";
        String templateEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntity;";

        String idTypeDesc = "L" + idType.getName().replace('.', '/') + ";";
        String templateIdTypeDesc = "Ljava/lang/Object;";

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("erp/redis/interfaceimplementer/TemplateEntityRepositoryImpl.class");
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
                mthDesc = mthDesc.replaceAll(templateEntityTypeDesc, entityTypeDesc).replaceAll(templateIdTypeDesc, idTypeDesc);
                return new AdviceAdapter(Opcodes.ASM5, super.visitMethod(access, mthName, mthDesc, signature, exceptions), access, mthName, mthDesc) {
                    @Override
                    public void visitTypeInsn(final int opcode, final String type) {
                        String realType = type;
                        if (Opcodes.CHECKCAST == opcode) {
                            realType = entityType.getName().replace('.', '/');
                        }
                        super.visitTypeInsn(opcode, realType);
                    }
                };
            }
        }, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }

    private static byte[] generateNewClsBytesForGeneric(Class itfType, Class entityType) {
        String entityTypeDesc = "L" + entityType.getName().replace('.', '/') + ";";
        String templateEntityTypeDesc = "Lerp/redis/interfaceimplementer/TemplateEntity;";

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("erp/redis/interfaceimplementer/GenericTemplateEntityRepositoryImpl.class");
        byte[] bytes = new byte[0];
        try {
            bytes = new byte[is.available()];
            is.read(bytes);
        } catch (IOException e) {
            throw new RuntimeException("read GenericTemplateEntityRepositoryImpl.class error", e);
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
                mthDesc = mthDesc.replaceAll(templateEntityTypeDesc, entityTypeDesc);
                return new AdviceAdapter(Opcodes.ASM5, super.visitMethod(access, mthName, mthDesc, signature, exceptions), access, mthName, mthDesc) {
                    @Override
                    public void visitTypeInsn(final int opcode, final String type) {
                        String realType = type;
                        if (Opcodes.CHECKCAST == opcode) {
                            realType = entityType.getName().replace('.', '/');
                        }
                        super.visitTypeInsn(opcode, realType);
                    }
                };
            }
        }, ClassReader.EXPAND_FRAMES);
        return cw.toByteArray();
    }
}
