import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

public class Avro2904Test {
    // If you just used Instant.now() test would randomly pass 1 times out of a 1000.
    // If you use Instant.now().truncatedTo(ChronoUnit.MILLIS) it would always pass.
    // By making sure micros is non-zero we ensure it always fails and demonstrates AVRO-2904.
    private Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS).plus(999, ChronoUnit.MICROS);

    @Test
    public void test1Builder() throws IOException {
        final TimestampTest1 t = TimestampTest1.newBuilder().setTime(now).build();
        byte[] bytes = serialize(t);
        TimestampTest1 other = deserialize(t.getSchema(), bytes);
        // fails because of AVRO-2904
        assertEquals(t, other);
    }

    @Test
    public void test1Constructor() throws IOException {
        final TimestampTest1 t = new TimestampTest1(now);
        byte[] bytes = serialize(t);
        TimestampTest1 other = deserialize(t.getSchema(), bytes);
        // fails because of AVRO-2904
        assertEquals(t, other);
    }

    @Test
    public void test2Builder() throws IOException {
        final TimestampTest2 t = TimestampTest2.newBuilder().setTime(Instant.now()).build();
        byte[] bytes = serialize(t);
        TimestampTest2 other = deserialize(t.getSchema(), bytes);
        // if logicalType timestamp-millis in not in union with null it works fine
        assertEquals(t, other);
    }

    @Test
    public void test2Constructor() throws IOException {
        final TimestampTest2 t = new TimestampTest2(now);
        byte[] bytes = serialize(t);
        TimestampTest2 other = deserialize(t.getSchema(), bytes);
        // if logicalType timestamp-millis in not in union with null it works fine
        assertEquals(t, other);
    }

    private byte[] serialize(SpecificRecord record) throws IOException {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(
                record.getSchema());
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            final byte[] bytes = out.toByteArray();
            return bytes;
        }
    }

    private <T extends SpecificRecord> T deserialize(Schema schema, byte[] bytes)
            throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
        return reader.read(null, decoder);
    }
}
