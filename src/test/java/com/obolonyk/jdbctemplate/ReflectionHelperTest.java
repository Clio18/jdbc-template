package com.obolonyk.jdbctemplate;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import static com.obolonyk.jdbctemplate.ReflectionHelper.*;
import static org.junit.jupiter.api.Assertions.*;

class ReflectionHelperTest {
    private final Boolean booleanArg = true;
    private final Character charArg = 'c';
    private final Byte byteArg = 2;
    private final Short shortArg = 2;
    private final Integer intArg = 2;
    private final Long longArg = 2L;
    private final Float floatArg = 2.1f;
    private final Double doubleArg = 2.1;
    private final String strArg = "str";
    private final Timestamp timestampArg = Timestamp.valueOf(LocalDateTime.now());
    private final LocalDateTime localDateTimeArg = LocalDateTime.now();


    @Test
    void testIsWrapperTrue() {
        assertTrue(isWrapperType(booleanArg.getClass()));
        assertTrue(isWrapperType(charArg.getClass()));
        assertTrue(isWrapperType(byteArg.getClass()));
        assertTrue(isWrapperType(shortArg.getClass()));
        assertTrue(isWrapperType(intArg.getClass()));
        assertTrue(isWrapperType(longArg.getClass()));
        assertTrue(isWrapperType(floatArg.getClass()));
        assertTrue(isWrapperType(doubleArg.getClass()));
    }

    @Test
    void testIsWrapperFalse() {
        assertFalse(isWrapperType(strArg.getClass()));
        assertFalse(isWrapperType(timestampArg.getClass()));
    }

    @Test
    void testGetSetterNameAndClassNameWrapperType() {
        Map<String, Object> map = getSetterNameAndClassName(longArg);
        assertFalse(map.isEmpty());

        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(1, entrySet.size());

        for (Map.Entry<String, Object> entry : entrySet) {
            assertEquals("setLong", entry.getKey());
            assertEquals(longArg, entry.getValue());
        }
    }

    @Test
    void testGetSetterNameAndClassNameStringType() {
        Map<String, Object> map = getSetterNameAndClassName(strArg);
        assertFalse(map.isEmpty());

        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(1, entrySet.size());

        for (Map.Entry<String, Object> entry : entrySet) {
            assertEquals("setString", entry.getKey());
            assertEquals(strArg, entry.getValue());
        }
    }

    @Test
    void testGetSetterNameAndClassNameIntegerType() {
        Map<String, Object> map = getSetterNameAndClassName(intArg);
        assertFalse(map.isEmpty());

        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(1, entrySet.size());

        for (Map.Entry<String, Object> entry : entrySet) {
            assertEquals("setInt", entry.getKey());
            assertEquals(intArg, entry.getValue());
        }
    }

    @Test
    void testGetSetterNameAndClassNameLocalDateTimeType() {
        Map<String, Object> map = getSetterNameAndClassName(localDateTimeArg);
        assertFalse(map.isEmpty());

        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        assertEquals(1, entrySet.size());

        for (Map.Entry<String, Object> entry : entrySet) {
            assertEquals("setTimestamp", entry.getKey());
            Timestamp timestamp = Timestamp.valueOf(localDateTimeArg);
            assertEquals(timestamp, entry.getValue());
        }
    }

    @Test
    void testUnwrapGetClassTypeWrapperClass(){
        Class<?> typeLongPrimitive = getUnwrapClassType(longArg);
        Class<?> typeIntPrimitive = getUnwrapClassType(intArg);
        Class<?> typeBytePrimitive = getUnwrapClassType(byteArg);
        Class<?> typeDoublePrimitive = getUnwrapClassType(doubleArg);
        Class<?> typeCharPrimitive = getUnwrapClassType(charArg);

        assertEquals(long.class, typeLongPrimitive);
        assertEquals(int.class, typeIntPrimitive);
        assertEquals(byte.class, typeBytePrimitive);
        assertEquals(double.class, typeDoublePrimitive);
        assertEquals(char.class, typeCharPrimitive);
    }

    @Test
    void testUnwrapGetClassType(){
        Class<?> typeStr = getUnwrapClassType(strArg);
        assertEquals(String.class, typeStr);
    }

}