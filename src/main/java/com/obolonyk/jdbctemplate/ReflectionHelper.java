package com.obolonyk.jdbctemplate;

import lombok.SneakyThrows;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReflectionHelper {
    private static final String SETTER_PREFIX = "set";
    private static final String INT_SUFFIX = "Int";

    private static final Set<Class<?>> WRAPPER_TYPES = getWrapperTypes();
    private static final Map<Class<?>, Class<?>> WRAPPER_PRIMITIVES_TYPES = getWrapperToPrimitiveClasses();

    public static boolean isWrapperType(Class<?> clazz) {
        return WRAPPER_TYPES.contains(clazz);
    }

    private static Set<Class<?>> getWrapperTypes() {
        Set<Class<?>> ret = new HashSet<>();
        ret.add(Boolean.class);
        ret.add(Character.class);
        ret.add(Byte.class);
        ret.add(Short.class);
        ret.add(Integer.class);
        ret.add(Long.class);
        ret.add(Float.class);
        ret.add(Double.class);
        return ret;
    }

    private static Map<Class<?>, Class<?>> getWrapperToPrimitiveClasses() {
        Map<Class<?>, Class<?>> map = new HashMap<>();
        map.put(Boolean.class, Boolean.TYPE);
        map.put(Character.class, Character.TYPE);
        map.put(Byte.class, Byte.TYPE);
        map.put(Short.class, Short.TYPE);
        map.put(Integer.class, Integer.TYPE);
        map.put(Long.class, Long.TYPE);
        map.put(Float.class, Float.TYPE);
        map.put(Double.class, Double.TYPE);
        return map;
    }

    @SneakyThrows
    public static void initPreparedStatement(PreparedStatement preparedStatement, Map<String, Object> data, int index) {
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        for (Map.Entry<String, Object> entry : entrySet) {
            Object value = entry.getValue();
            Class<?> unwrappedClass = getUnwrapClassType(value);

            Class<? extends PreparedStatement> preparedStatementClass = preparedStatement.getClass();
            Method method = preparedStatementClass.getDeclaredMethod(entry.getKey(), int.class, unwrappedClass);
            method.invoke(preparedStatement, index, entry.getValue());
        }

    }

    static Class<?> getUnwrapClassType(Object value) {
        Class<?> unwrappedClass;
        if (isWrapperType(value.getClass())) {
            unwrappedClass = WRAPPER_PRIMITIVES_TYPES.get(value.getClass());
        } else {
            unwrappedClass = value.getClass();
        }
        return unwrappedClass;
    }

    public static Map<String, Object> getSetterNameAndClassName(Object arg) {
        Map<String, Object> map = new HashMap<>(1);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(SETTER_PREFIX);
        Class<?> aClass = arg.getClass();

        if (aClass == Integer.class) {
            stringBuilder.append(INT_SUFFIX);
            map.put(stringBuilder.toString(), arg);
            return map;
        }
        if (aClass == LocalDateTime.class) {
            Timestamp value = Timestamp.valueOf((LocalDateTime) arg);
            String simpleName = value.getClass().getSimpleName();
            String setterName = stringBuilder.append(simpleName).toString();
            map.put(setterName, value);
            return map;
        }

        String name = aClass.getSimpleName();
        String setterName = stringBuilder.append(name).toString();
        map.put(setterName, arg);
        return map;
    }
}
