/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.achalaggarwal.workerbee.tools;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HiveLocalMetaStore;
import com.github.sakserv.minicluster.impl.HiveLocalServer2;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import com.github.sakserv.propertyparser.PropertyParser;
import lombok.Getter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LocalHiveServer {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(LocalHiveServer.class);

    private ZookeeperLocalCluster zookeeperLocalCluster;
    private HiveLocalMetaStore hiveLocalMetaStore;
    private HiveLocalServer2 hiveLocalServer2;

    @Getter
    private String jdbcURL;

    public LocalHiveServer() throws Exception {
        this(ConfigVars.DEFAULT_PROPS_FILE);
    }

    public LocalHiveServer(String propertyFilePath) throws Exception {
        this(readProperties(propertyFilePath));
    }

    public LocalHiveServer(PropertyParser propertyParser) throws Exception {
        this(propertyParser, buildHiveConf(new HiveConf()));
    }

    public LocalHiveServer(HiveConf hiveConf) throws Exception {
        this(readProperties(ConfigVars.DEFAULT_PROPS_FILE), buildHiveConf(hiveConf));
    }

    private static PropertyParser readProperties(String propertyFilePath) throws IOException {
        PropertyParser propertyParser = null;
        try {
            propertyParser = new PropertyParser(propertyFilePath);
            propertyParser.parsePropsFile();
        } catch(IOException e) {
            LOG.error("Unable to load property file: {}", propertyFilePath);
            throw e;
        }

        return propertyParser;
    }

    public LocalHiveServer(PropertyParser p, HiveConf hiveConf) throws Exception {

        zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
                .setPort(Integer.parseInt(p.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
                .setTempDir(p.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
                .setZookeeperConnectionString(p.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();

        hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
                .setHiveMetastoreHostname(p.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(p.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)) + 50)
                .setHiveMetastoreDerbyDbDir(p.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(p.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(p.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(hiveConf)
                .build();

        hiveLocalServer2 = new HiveLocalServer2.Builder()
                .setHiveServer2Hostname(p.getProperty(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY))
                .setHiveServer2Port(Integer.parseInt(p.getProperty(ConfigVars.HIVE_SERVER2_PORT_KEY)))
                .setHiveMetastoreHostname(p.getProperty(ConfigVars.HIVE_METASTORE_HOSTNAME_KEY))
                .setHiveMetastorePort(Integer.parseInt(p.getProperty(ConfigVars.HIVE_METASTORE_PORT_KEY)) + 50)
                .setHiveMetastoreDerbyDbDir(p.getProperty(ConfigVars.HIVE_METASTORE_DERBY_DB_DIR_KEY))
                .setHiveScratchDir(p.getProperty(ConfigVars.HIVE_SCRATCH_DIR_KEY))
                .setHiveWarehouseDir(p.getProperty(ConfigVars.HIVE_WAREHOUSE_DIR_KEY))
                .setHiveConf(hiveConf)
                .setZookeeperConnectionString(p.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
                .build();

        jdbcURL = String.format(
            "jdbc:hive2://%s:%s/",
            p.getProperty(ConfigVars.HIVE_SERVER2_HOSTNAME_KEY),
            p.getProperty(ConfigVars.HIVE_SERVER2_PORT_KEY)
        );


    }

    public LocalHiveServer start() throws Exception {
        zookeeperLocalCluster.start();
        hiveLocalMetaStore.start();
        hiveLocalServer2.start();

        LOG.info("Started HS2. Use {} to connect.", jdbcURL);
        return this;
    }

    public LocalHiveServer cleanUp() throws Exception {
        zookeeperLocalCluster.cleanUp();
        hiveLocalMetaStore.cleanUp();
        hiveLocalServer2.cleanUp();

        return this;
    }

    public LocalHiveServer stop() throws Exception {
        hiveLocalServer2.stop();
        hiveLocalMetaStore.stop();
        zookeeperLocalCluster.stop();

        return this;
    }

    public static HiveConf buildHiveConf(HiveConf hiveConf) {
        // Handle Windows
        WindowsLibsUtils.setHadoopHome();

        hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_ON.varname, "true");
        hiveConf.set(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_THREADS.varname, "5");
        hiveConf.set("hive.root.logger", "DEBUG,console");
        hiveConf.set(HiveConf.ConfVars.SUBMITLOCALTASKVIACHILD.varname, "false");
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
        System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
        return hiveConf;
    }
}
