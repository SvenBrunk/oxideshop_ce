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
 * 1. Master picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called.
 * 2. Slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used.
 * 3. If master was picked once during the lifetime of the connection it will always get picked afterwards.
 * 4. One slave connection is randomly picked ONCE during a request.
 *
 * This integration test suite covers all methods of \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database that are using
 * one of the before mentioned methods of \Doctrine\DBAL\Connection under the hood. Their behavior is determined by the
 * doctrine functions and they should react in the same way.
 *
 * To test the master slave behaviour, 3 database connections are established:
 * - a connection to the slave database
 * - a connection to the master database
 * - a master-slave connection, which is expected to switch between master and slave according the rules mentioned before .
 *
 * All tests are completely independent from each other and always start with a fresh database table and fresh connections.
 *
 * \Doctrine\DBAL\Connection::executeQuery
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

    private $expectedMasterResult;

    /**
     * @inheritdoc
     */
    public function setUp()
    {
        parent::setUp();

        /**
         * Setup the different connections
         */
        /** Set db property of Database instance to null to enforce a fresh connection */
        $this->setProtectedClassProperty(Database::getInstance(), 'db' , null);
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

        $this->expectedMasterResult = '1';

        /** Insert record to the master database */
        $this->populateTestTable($this->expectedMasterResult);
    }

    /**
     * @inheritdoc
     */
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

    /**
     * Assure that replication works at all
     */
    public function testBasicMasterSlaveFunctionality()
    {
        /**
         * Act
         */
        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /**
         * Assert
         */
        $this->assertSame($this->expectedMasterResult, $actualSlaveResult, 'Slave connection retrieves record inserted into master database');
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves record inserted into master database');
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

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        $actualMasterSlaveResult = $this->masterSlaveConnection->getAll('SELECT column1 FROM `master_slave_table` WHERE id = 1')[0]['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection reads from slave database when using getAll()');
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
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */
        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        $actualMasterSlaveResult = $this->masterSlaveConnection->getCol('SELECT column1 FROM `master_slave_table` WHERE id = 1')[0];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection reads from slave database when using getCol()');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getOne uses executeQuery under the hood to retrieve the results.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getOne
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnGetOne()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */
        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        $actualMasterSlaveResult = $this->masterSlaveConnection->getOne('SELECT column1 FROM `master_slave_table` WHERE id = 1');

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection reads from slave database when using getOne()');
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
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        $actualMasterSlaveResult = $this->masterSlaveConnection->getRow('SELECT column1 FROM `master_slave_table` WHERE id = 1')['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection reads from slave database when using getRow()');
    }

    /**
     * Test Doctrine behavior:
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select uses executeQuery under the hood.
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::selectLimit uses select under the hood.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::select
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::selectLimit
     */
    public function testMasterSlaveConnectionReadsFromSlaveOnSelect()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** @var  Database\Adapter\ResultSetInterface $resultSet */
        $resultSet = $this->masterSlaveConnection->select('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $actualMasterSlaveResult = $resultSet->fields['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection reads from slave database when using select()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getAll uses executeQuery under the hood to retrieve the results.
     * Now picking the master is forced by using 'executeUpdate' and then the master should also be also used for
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getAll
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getAll
     */
    public function testMasterSlaveConnectionReadsFromMasterOnGetAll()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        $actualMasterSlaveResult = $this->masterSlaveConnection->getAll('SELECT column1 FROM `master_slave_table` WHERE id = 1')[0]['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database when using getAll() after execute()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getCol uses executeQuery under the hood to retrieve the results.
     * Now picking the master is forced by using 'executeUpdate' and then the master should also be also used for
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getCol
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getCol
     */
    public function testMasterSlaveConnectionReadsFromMasterOnGetCol()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        $actualMasterSlaveResult = $this->masterSlaveConnection->getCol('SELECT column1 FROM `master_slave_table` WHERE id = 1')[0];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database when using getCol() after execute()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getOne uses executeQuery under the hood to retrieve the results.
     * Now picking the master is forced by using 'executeUpdate' and then the master should also be also used for
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getOne
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getOne
     */
    public function testMasterSlaveConnectionReadsFromMasterOnGetOne()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        $actualMasterSlaveResult = $this->masterSlaveConnection->getOne('SELECT column1 FROM `master_slave_table` WHERE id = 1');

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database when using getOne() after execute()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     * "Use slave if master was never picked before and ONLY if 'getWrappedConnection' or 'executeQuery' is used."
     *
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getRow uses executeQuery under the hood to retrieve the results.
     * Now picking the master is forced by using 'executeUpdate' and then the master should also be also used for
     * \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getRow
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::getRow
     */
    public function testMasterSlaveConnectionReadsFromMasterOnGetRow()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        $actualMasterSlaveResult = $this->masterSlaveConnection->getRow('SELECT column1 FROM `master_slave_table` WHERE id = 1')['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database when using getRow() after execute()');
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
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** Force picking the master by doing an execute here. Doctrine:executeUpdate is called under the hood */
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        /** @var  Database\Adapter\ResultSetInterface $resultSet */
        $resultSet = $this->masterSlaveConnection->select('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $actualMasterSlaveResult = $resultSet->fields['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $this->expectedMasterResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database when using select() after execute()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     *
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::startTransaction uses beginTransaction under the hood.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::startTransaction
     */
    public function testMasterSlaveConnectionReadsFromMasterDuringTransaction()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

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
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $this->expectedMasterResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database after using startTransaction()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     *
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::startTransaction uses beginTransaction under the hood.
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::commitTransaction
     */
    public function testMasterSlaveConnectionReadsFromMasterAfterCommitTransaction()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';

        /**
         * Act
         */

        /** Start a transaction, do an update and commit it. */
        $this->masterSlaveConnection->startTransaction();
        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (2, 3)');
        $this->masterSlaveConnection->commitTransaction();

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /**
         * Read from the master-slave connection. In this case the modifications on the slave should be invisible
         */
        $actualMasterSlaveResult = $this->masterSlaveConnection->getOne('SELECT column1 FROM `master_slave_table` WHERE id = 1');

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $this->expectedMasterResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterSlaveResult, 'Master-Slave connection reads from master database after using commitTransaction()');
    }

    /**
     * Test Doctrine behavior:
     * "Master is picked when 'exec', 'executeUpdate', 'insert', 'delete', 'update', 'createSavepoint', 'releaseSavepoint', 'beginTransaction', 'rollback', 'commit', 'query' or 'prepare' is called."
     *
     *  \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::execute uses executeUpdate under the hood.
     *
     * In this test the slave database is use first for reading.
     * The and update is made and the master database should be used for writing
     *
     * @covers \OxidEsales\Eshop\Core\Database\Adapter\Doctrine\Database::execute
     */
    public function testMasterSlaveConnectionWritesToMasterOnExecute()
    {
        /**
         * Arrange
         */
        $expectedSlaveResult = '100';
        $expectedMasterResultAfterExecute = '2';

        /**
         * Act
         */

        /** Modify record directly on the slave database, bypassing master-slave replication */
        $this->slaveConnection->query('UPDATE `master_slave_table` SET `column1` = ' . $expectedSlaveResult . ' WHERE id = 1');

        /** @var \mysqli_result $resultSet Read directly from slave database */
        $actualSlaveResult = $this->readValueFromSlaveDb();

        /** @var \mysqli_result $resultSet Read directly from master database */
        $actualMasterResult = $this->readValueFromMasterDb();

        /** @var  Database\Adapter\ResultSetInterface $resultSet */
        $resultSet = $this->masterSlaveConnection->select('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $actualMasterSlaveResult = $resultSet->fields['column1'];

        $this->masterSlaveConnection->execute('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResultAfterExecute . ', 3)');

        /** @var \mysqli_result $resultSet Read directly from master database */
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 2');
        $row = $resultSet->fetch_assoc();
        $actualMasterResultAfterExecute = $row['column1'];

        /**
         * Assert
         */
        $this->assertSame($expectedSlaveResult, $actualSlaveResult, 'Slave connection retrieves modified value ' . $expectedSlaveResult);
        $this->assertSame($this->expectedMasterResult, $actualMasterResult, 'Master connection retrieves unmodified value ' . $expectedSlaveResult);
        $this->assertSame($expectedSlaveResult, $actualMasterSlaveResult, 'Master-Slave connection retrieves modified value ' . $expectedSlaveResult . ' from slave database on select');
        $this->assertSame($expectedMasterResultAfterExecute, $actualMasterResultAfterExecute, 'Master-Slave connection writes new value ' . $this->expectedMasterResult . ' to master database using execute');
    }

    public function testMetaColumnsReadsFromMaster() {
        /**
         * Arrange
         */
        $expectedResult = $this->masterSlaveConnection->metaColumns('master_slave_table');

        /**
         * Modify the table on the slave database to see if the change is visible to metaColumns and thus the table
         * definition is read from the slave instead of the master database
         */
        $this->slaveConnection->query('ALTER TABLE `master_slave_table`	ADD COLUMN `column3` INT NULL AFTER `column2`;');

        /*
         * Act
         */
        $actualResult = $this->masterSlaveConnection->metaColumns('master_slave_table');

        /**
         * Assert
         */
        $this->assertEquals($expectedResult, $actualResult);
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

    /**
     * @return array
     */
    protected function readValueFromSlaveDb()
    {
        $resultSet = $this->slaveConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $value = $row['column1'];

        return $value;
    }

    /**
     * @return mixed
     */
    protected function readValueFromMasterDb()
    {
        $resultSet = $this->masterConnection->query('SELECT column1 FROM `master_slave_table` WHERE id = 1');
        $row = $resultSet->fetch_assoc();
        $value = $row['column1'];

        return $value;
    }

    /**
     * @param $expectedMasterResult
     */
    protected function populateTestTable($expectedMasterResult)
    {

        $this->masterConnection->query('INSERT INTO `master_slave_table` (`column1`, `column2`) VALUES (' . $expectedMasterResult . ', 2)');
        /** Pause to let replication take place */
        sleep(2);
    }


    /**
     * Set a given protected property of a given class instance to a given value.
     *
     * @param object $classInstance Instance of the class of which the property will be set
     * @param string $property      Name of the property to be set
     * @param mixed  $value         Value to which the property will be set
     */
    protected function setProtectedClassProperty($classInstance, $property, $value)
    {
        $className = get_class($classInstance);

        $reflectionClass = new \ReflectionClass($className);

        $reflectionProperty = $reflectionClass->getProperty($property);
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($classInstance, $value);
    }
}
