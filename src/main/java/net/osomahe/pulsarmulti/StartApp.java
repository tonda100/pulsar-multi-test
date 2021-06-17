package net.osomahe.pulsarmulti;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class StartApp {

    public static void main(String[] args) {
        Quarkus.run(TestPulsar.class, args);
    }

}
