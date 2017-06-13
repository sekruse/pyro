package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.PartialFD;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * This utility reads and writes functional dependencies from and to disk. The format is particularly human-readable:
 * <p>
 * {@code ["column 1", "column 2"] -> "column 3" # optional comments}
 * </p>
 */
public class FDPersistence {

    public static Writer createWriter(String file) throws FileNotFoundException {
        return new Writer(new FileOutputStream(file, false));
    }

    /**
     * Writes FDs in a human-readable format.
     */
    public static class Writer implements AutoCloseable {

        private final BufferedWriter writer;

        private Writer(OutputStream outputStream) {
            this.writer = new BufferedWriter(new OutputStreamWriter(outputStream, Charset.forName("UTF-8")));
        }

        public void write(PartialFD partialFD) throws IOException {
            this.write(
                    new PlainTextFD(partialFD),
                    String.format("error: %,.5f, error: %,.5f", partialFD.error, partialFD.score)
            );
        }

        public void write(PlainTextFD plainTextFD, String comment) throws IOException {
            this.write(plainTextFD.lhs, plainTextFD.rhs, comment);
        }

        private void write(List<String> lhs, String rhs, String comment) throws IOException {
            this.writer.write('[');
            String separator = "";
            for (String column : lhs) {
                this.writer.write(separator);
                this.writeEscaped(column);
                separator = ", ";
            }
            this.writer.write("] -> ");
            this.writeEscaped(rhs);
            if (comment != null && !comment.isEmpty()) {
                this.writer.write(" # ");
                this.writer.write(comment.replaceAll("[\n\r]", " "));
            }
            this.writer.newLine();
        }

        private void writeEscaped(String column) throws IOException {
            this.writer.write('"');
            for (int i = 0; i < column.length(); i++) {
                char ch = column.charAt(i);
                switch (ch) {
                    case '\\':
                    case '"':
                        this.writer.write('\\');
                    default:
                        this.writer.write(ch);

                }
            }
            this.writer.write('"');
        }

        @Override
        public void close() throws Exception {
            try {
                this.writer.close();
            } catch (Exception e) {
                // Just report, but ignore otherwise.
                e.printStackTrace();
            }
        }
    }

    public static Reader createReader(String path) throws FileNotFoundException {
        File file = new File(path);
        if (!file.isFile()) {
            throw new IllegalArgumentException("No such file: " + path);
        }
        return new Reader(new FileInputStream(file));
    }


    /**
     * Reads FDs in a human-readable format.
     */
    public static class Reader implements AutoCloseable {

        private final BufferedReader reader;

        private int readPos;

        private Reader(InputStream inputStream) {
            this.reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
        }

        public PlainTextFD read() throws IOException {
            String line = this.reader.readLine();
            if (line == null) return null;

            this.readPos = 0;
            expect(line, this.readPos, "[");
            this.readPos++;
            List<String> lhs = new ArrayList<>();
            while (true) {
                expectCharAt(line, readPos);
                if (line.charAt(readPos) == ']') {
                    expect(line, this.readPos, "] -> ");
                    readPos += 5;
                    break;
                }

                lhs.add(this.readEscaped(line));
                expectCharAt(line, readPos);
                if (line.charAt(readPos) == ',') {
                    expect(line, this.readPos, ", ");
                    readPos += 2;
                }
            }

            String rhs = this.readEscaped(line);

            return new PlainTextFD(lhs, rhs);

        }

        /**
         * Expect a certain text in the line.
         */
        private void expect(String line, int from, String expected) {
            for (int i = 0; i < expected.length(); i++) {
                char expectedChar = expected.charAt(i);
                if (from + i >= line.length()) {
                    throw new RuntimeException(
                            String.format("Premature end of line \"%s\": expected \"%s\".", line, expected)
                    );
                }
                char actualChar = line.charAt(from + i);
                if (expectedChar != actualChar) {
                    throw new RuntimeException(
                            String.format(
                                    "Illegal format in line \"%s\": expected \"%s\" but found \"%s\".",
                                    line, expectedChar, actualChar
                            )
                    );
                }
            }
        }

        /**
         * Read a double-quoted, escaped sequence.
         */
        private String readEscaped(String line) throws IOException {
            StringBuilder sb = new StringBuilder();
            this.expect(line, this.readPos, "\"");
            this.readPos++;
            while (true) {
                expectCharAt(line, this.readPos);
                char ch = line.charAt(this.readPos);
                if (ch == '\\') {
                    this.readPos++;
                    expectCharAt(line, this.readPos);
                    ch = line.charAt(this.readPos);
                    // Lenient: We don't check if the escaped character is a \ or a ".
                } else if (ch == '"') {
                    this.readPos++;
                    break;
                }
                this.readPos++;
                sb.append(ch);
            }
            return sb.toString();
        }

        /**
         * Expect the existence of a character.
         */
        private void expectCharAt(String line, int pos) {
            if (pos >= line.length()) {
                throw new RuntimeException(
                        String.format(
                                "mature end of line \"%s\": expected character at position %d.", line, pos
                        )
                );
            }
        }

        @Override
        public void close() {
            try {
                this.reader.close();
            } catch (Exception e) {
                // Just report, but ignore otherwise.
                e.printStackTrace();
            }
        }
    }

}
