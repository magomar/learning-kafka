package org.learning.kafka;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigTest  {

    @Test
    void integerConfigProperty() {
        assertThat(Config.KAFKA_BATCH_SIZE.get(), instanceOf(Integer.class));
        assertEquals(Integer.valueOf(32), Config.KAFKA_BATCH_SIZE.get());
    }

}
