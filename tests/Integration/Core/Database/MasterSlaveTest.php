<?php
/**
 * This file is part of OXID eShop Community Edition.
 *
 * OXID eShop Community Edition is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * OXID eShop Community Edition is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OXID eShop Community Edition.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @link          http://www.oxid-esales.com
 * @copyright (C) OXID eSales AG 2003-2016
 * @version       OXID eShop CE
 */
namespace Integration\Core\Database;

use OxidEsales\Eshop\Core\Database;
use OxidEsales\Eshop\Core\Database\Adapter\DatabaseInterface;
use OxidEsales\TestingLibrary\UnitTestCase;

/**
 * Class MasterSlaveTest
 *
 * @group  database-adapter
 * @group  master-slave
 * @covers OxidEsales\Eshop\Core\Database
 *
 * Quote form the Doctrine API documentation:
 *
 * Important for the understanding of this connection should be how and when it picks the slave or master.
 *
 * 1. Slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used.
 * 2. Master picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called.
 * 3. If master was picked once during the lifetime of the connection it will always get picked afterwards.
 * 4. One slave connection is randomly picked ONCE during a request.
 *
 * ATTENTION: You can write to the slave with this connection if you execute a write query without opening up a transaction. For example:
 * $conn = DriverManager::getConnection(...); $conn->executeQuery("DELETE FROM table");
 *
 * Be aware that Connection#executeQuery is a method specifically for READ operations only.
 *
 * This connection is limited to slave operations using the Connection#executeQuery operation only, because it wouldn't be compatible with the ORM or SchemaManager code otherwise. Both use all the other operations in a context where writes could happen to a slave, which makes this restricted approach necessary.
 */
class DatabaseTest extends UnitTestCase
{

    /**
     * @var DatabaseInterface
     */
    private $masterSlaveConnection;

    /**
     * @var \mysqli
     */
    private $masterConnection;

    /**
     * @var \mysqli
     */
    private $slaveConnection;

    public function setUp()
    {
        parent::setUp();

        /**
         * Setup the different connections
         */
        $this->masterSlaveConnection = $this->getConnectionMasterSlave();
        $this->masterConnection = $this->getConnectionMaster();
        $this->slaveConnection = $this->getConnectionSlave();

        /**
         * Drop the test table before each test if it exists
         */
        $this->dropMasterSlaveTable($this->masterConnection);
        /**
         * Create a test table before each test
         */
        $this->createMasterSlaveTable($this->masterConnection);
    }

    public function tearDown()
    {
        parent::tearDown();

        $this->masterSlaveConnection->closeConnection();
        $this->masterConnection->close();
        $this->slaveConnection->close();

        $this->masterSlaveConnection = null;
        $this->masterConnection = null;
        $this->slaveConnection = null;

    }

    /**
     * This test will fail if the connection could not be established
     */
    public function testConnectionMaster()
    {
        $this->getConnectionMaster();
    }

    /**
     * This test will fail if the connection could not be established
     */
    public function testConnectionSlave()
    {
        $this->getConnectionSlave();
    }

    /** Assure that replication works at all */
    public function testBasicMasterSlaveFunctionality()
    {
        /**
         * Arrange
         */
        $expectedMasterResult = '1';

        /** Insert record  to the master database */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /**
         * Act
         */
        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult = $row['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedMasterResult, $actualSlaveResult, 'Slave connection retrieves record inserted into master database');
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves record inserted into master database');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getAll uses executeQuery under the hood to retrieve the results.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getAll
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnGetAll()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult =  $row['column1'];

        $actualMasterSlaveResult = $this->masterSlaveConnection->getAll('SELECT column1 FROM `master_slave_table` WHERE id = 1')['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves modified value ' . $expectedSlaveResult . ' from slave database');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getCol uses executeQuery under the hood to retrieve the results.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getCol
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnGetCol()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult = $row['column1'];

        $actualMasterSlaveResult = $this->masterSlaveConnection->getCol('SELECT column1 FROM `master_slave_table` WHERE id = 1')[0];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves modified value ' . $expectedSlaveResult . ' from slave database');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getRow uses executeQuery under the hood to retrieve the results.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getRow
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnGetRow()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult =  $row['column1'];

        $actualMasterSlaveResult = $this->masterSlaveConnection->getRow('SELECT column1 FROM `master_slave_table` WHERE id = 1')['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves modified value ' . $expectedSlaveResult . ' from slave database');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select uses executeQuery to retrieve the results.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnSelect()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult = $row['column1'];

        /** @var  Database\Adapter\ResultSetInterface $resultSet */
        $resultSet = $this->masterSlaveConnection->select('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $actualMasterSlaveResult = $resultSet->fields['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves modified value ' . $expectedSlaveResult . ' from slave database');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select uses executeQuery to retrieve the results.
     * Now picking the master is forced by using 'executeUpdate' and then the master should also be also used for
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select
     */
    public function testMasterSlaveConnectionReadsFromMasterOnSelect()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult = $row['column1'];

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        /** @var  Database\Adapter\ResultSetInterface $resultSet */
        $resultSet = $this->masterSlaveConnection->select('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $actualMasterSlaveResult = $resultSet->fields['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves unmodified value ' . $expectedMasterResult . ' from master database');
    }

    public function testMasterSlaveConnectionUsesMasterForReadingDuringTransaction()
    {
        /**
         * Act
         */
        $expectedSlaveResult = '100';
        $expectedMasterResult = '1';

        /** Insert record using master-slave replication */
        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualSlaveResult = $row['column1'];

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $actualMasterResult = $row['column1'];

        /**
         * Start a transaction and read from the master-slave connection.
         * In this case the modifications on the slave should be invisible
         */
        $this->masterSlaveConnection->startTransaction();
        $actualMasterSlaveResult = $this->masterSlaveConnection->getOne('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $this->masterSlaveConnection->commitTransaction();

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedMasterResult);
        $this->assertSame($expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves unmodified value ' . $expectedMasterResult . ' from master database');
    }

    /**
     * Get a dedicated database connection to the master
     */
    protected function getConnectionMaster()
    {
        $configKeyDatabaseHost = 'dbHost';
        $configKeyDatabasePort = 'dbPort';
        $configKeyDatabaseName = 'dbName';
        $configKeyDatabaseUser = 'dbUser';
        $configKeyDatabasePassword = 'dbPwd';

        $mysqli = $this->getDatabaseConnection($configKeyDatabaseHost, $configKeyDatabasePort, $configKeyDatabaseName, $configKeyDatabaseUser, $configKeyDatabasePassword);

        return $mysqli;
    }

    /**
     * Get a dedicated database connection to the slave
     */
    protected function getConnectionSlave()
    {
        $configKeyDatabaseHost = 'aSlaveHosts';
        $configKeyDatabasePort = 'dbPort';
        $configKeyDatabaseName = 'dbName';
        $configKeyDatabaseUser = 'dbUser';
        $configKeyDatabasePassword = 'dbPwd';

        $mysqli = $this->getDatabaseConnection($configKeyDatabaseHost, $configKeyDatabasePort, $configKeyDatabaseName, $configKeyDatabaseUser, $configKeyDatabasePassword);

        return $mysqli;
    }

    /**
     * Get a master slave database connection
     */
    protected function getConnectionMasterSlave()
    {
        return Database::getDb(Database::FETCH_MODE_ASSOC);
    }

    /**
     * @param $configKeyDatabaseHost
     * @param $configKeyDatabasePort
     * @param $configKeyDatabaseName
     * @param $configKeyDatabaseUser
     * @param $configKeyDatabasePassword
     *
     * @return \mysqli
     */
    protected function getDatabaseConnection($configKeyDatabaseHost, $configKeyDatabasePort, $configKeyDatabaseName, $configKeyDatabaseUser, $configKeyDatabasePassword)
    {
        /** @var string $host Host name or IP address.
         * The config param for $configKeyDatabaseHost might be array in
         * case of a slave connection. In this case the first value of the array is chosen.
         */
        $host = is_array($this->getConfigParam($configKeyDatabaseHost)) ? $this->getConfigParam($configKeyDatabaseHost)[0] : $this->getConfigParam($configKeyDatabaseHost);
        $port = $this->getConfigParam($configKeyDatabasePort) ? $this->getConfigParam($configKeyDatabasePort) : 3306;
        $database = $this->getConfigParam($configKeyDatabaseName);
        $user = $this->getConfigParam($configKeyDatabaseUser);
        $password = $this->getConfigParam($configKeyDatabasePassword);

        $mysqli = new \mysqli($host, $user, $password, $database, $port);
        if ($mysqli->connect_error) {
            $this->fail(
                'Connect Error (' . $mysqli->connect_errno . ') '
                . $mysqli->connect_error
            );
        }

        return $mysqli;
    }

    /**
     * @param \mysqli $masterConnection
     *
     * @return string
     */
    protected function createMasterSlaveTable(\mysqli $masterConnection)
    {
        /** Create `master_slave_table` */
        $query = <<<EOT
                CREATE TABLE `master_slave_table` (
                    `id` INT(11) NOT NULL AUTO_INCREMENT,
                    `column1` INT(11) NULL DEFAULT NULL,
                    `column2` INT(11) NULL DEFAULT NULL,
                    PRIMARY KEY (`id`)
                )
                COMMENT='Temporary table to test master slave behaviour'
                COLLATE='latin1_general_ci'
                ENGINE=InnoDB
                ;
EOT;
        $masterConnection->query($query);

        return $query;
    }

    /**
     * @param \mysqli $masterConnection
     *
     * @return string
     */
    protected function dropMasterSlaveTable(\mysqli $masterConnection)
    {
        /** Remove `master_slave_table` */
        $query = "DROP TABLE IF EXISTS `master_slave_table`";
        $masterConnection->query($query);
    }
}
