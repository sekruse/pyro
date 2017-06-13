package de.hpi.isg.pyro.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * Test suite for the class {@link FDPersistence}.
 */
public class FDPersistenceTest {

    @Test
    public void testWritingAndReadingOnNormalData() throws Exception {
        File tempFile = File.createTempFile("hpi", "txt");
        tempFile.deleteOnExit();
        String path = tempFile.getAbsolutePath();

        Set<PlainTextFD> originalFDs = new HashSet<>();
        try (FDPersistence.Writer writer = FDPersistence.createWriter(path)) {
            PlainTextFD fd;
            originalFDs.add(fd = new PlainTextFD(new ArrayList<>(), "rhs"));
            writer.write(fd, null);
            originalFDs.add(fd = new PlainTextFD(Collections.singletonList("lhs"), "rhs"));
            writer.write(fd, "nice FD, though");
            originalFDs.add(fd = new PlainTextFD(Arrays.asList("lhs1", "lhs2"), "rhs"));
            writer.write(fd, "");
            originalFDs.add(fd = new PlainTextFD(Arrays.asList("lhs1", "lhs2", "lhs3"), "rhs"));
            writer.write(fd, "and a regular comment");
        }

        Set<PlainTextFD> loadedFDs = new HashSet<>();
        try (FDPersistence.Reader reader = FDPersistence.createReader(path)) {
            PlainTextFD fd;
            while ((fd = reader.read()) != null) {
                loadedFDs.add(fd);
            }
        }

        Assert.assertEquals(originalFDs, loadedFDs);

    }

    @Test
    public void testWritingAndReadingNastyData() throws Exception {
        File tempFile = File.createTempFile("hpi", "txt");
        tempFile.deleteOnExit();
        String path = tempFile.getAbsolutePath();

        Set<PlainTextFD> originalFDs = new HashSet<>();
        try (FDPersistence.Writer writer = FDPersistence.createWriter(path)) {
            PlainTextFD fd;
            originalFDs.add(fd = new PlainTextFD(new ArrayList<>(), "value"));
            writer.write(fd, null);
            originalFDs.add(fd = new PlainTextFD(Collections.singletonList(", "), "val\"ue"));
            writer.write(fd, "#\n#");
            originalFDs.add(fd = new PlainTextFD(Arrays.asList("", "bla"), "val\\ue"));
            writer.write(fd, "");
            originalFDs.add(fd = new PlainTextFD(Arrays.asList("", "bla", "blubb"), "value"));
            writer.write(fd, "and a regular comment");
        }

        Set<PlainTextFD> loadedFDs = new HashSet<>();
        try (FDPersistence.Reader reader = FDPersistence.createReader(path)) {
            PlainTextFD fd;
            while ((fd = reader.read()) != null) {
                loadedFDs.add(fd);
            }
        }

        Assert.assertEquals(originalFDs, loadedFDs);

    }

}