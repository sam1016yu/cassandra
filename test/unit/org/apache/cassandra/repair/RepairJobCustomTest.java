/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairJobCustomTest extends SchemaLoader
{
    private static final long TEST_TIMEOUT_S = 10;
    private static final long THREAD_TIMEOUT_MILLIS = 100;
    private static final IPartitioner MURMUR3_PARTITIONER = Murmur3Partitioner.instance;
    private static final String KEYSPACE = "RepairJobTest";
    private static final String CF = "Standard1";
    private static final Object messageLock = new Object();

    private static final List<Range<Token>> fullRange = Collections.singletonList(new Range<>(MURMUR3_PARTITIONER.getMinimumToken(),
                                                                                              MURMUR3_PARTITIONER.getRandomToken()));
    private static InetAddress addr1;
    private static InetAddress addr2;
    private static InetAddress addr3;
    private static InetAddress addr4;
    private MeasureableRepairSession session;
    private RepairJob job;
    private RepairJobDesc sessionJobDesc;

    private static class MeasureableRepairSession extends RepairSession
    {
        private final CountDownLatch validationCompleteReached = new CountDownLatch(1);

        private volatile boolean simulateValidationsOutstanding;

        public MeasureableRepairSession(UUID parentRepairSession, UUID id, Collection<Range<Token>> ranges, String keyspace,
                                        RepairParallelism parallelismDegree, Set<InetAddress> endpoints, long repairedAt, String... cfnames)
        {
            super(parentRepairSession, id, ranges, keyspace, parallelismDegree, endpoints, repairedAt, cfnames);
        }

        // So that threads actually get recycled and we can have accurate memory accounting while testing
        // memory retention from CASSANDRA-14096
        protected DebuggableThreadPoolExecutor createExecutor()
        {
            DebuggableThreadPoolExecutor executor = super.createExecutor();
            executor.setKeepAliveTime(THREAD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return executor;
        }

        void simulateValidationsOutstanding()
        {
            simulateValidationsOutstanding = true;
        }

        @Override
        public void validationComplete(RepairJobDesc desc, InetAddress endpoint, MerkleTrees trees)
        {
            validationCompleteReached.countDown();

            // Do not delegate the validation complete to parent to simulate that the call is still outstanding
            if (simulateValidationsOutstanding)
            {
                return;
            }
            super.validationComplete(desc, endpoint, trees);
        }

        void waitUntilReceivedFirstValidationComplete()
        {
            boolean isFirstValidationCompleteReceived = Uninterruptibles.awaitUninterruptibly(validationCompleteReached, TEST_TIMEOUT_S, TimeUnit.SECONDS);
            assertTrue("First validation completed", isFirstValidationCompleteReceived);
        }
    }

    @BeforeClass
    public static void setupClass() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
        addr1 = InetAddress.getByName("127.0.0.1");
    }

    private InetAddress getAddr(int i)  throws UnknownHostException
    {
        return InetAddress.getByName("127.0.0." + i);
    }


    public void setup(int numNeighbors)  throws UnknownHostException
    {
        
        
        Set<InetAddress> neighbors = new HashSet<>();

        for (int i=2; i<=1+numNeighbors; i++){
            neighbors.add(getAddr(i));
        }

        UUID parentRepairSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddress(),
                                                                 Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)), fullRange, false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false);

        this.session = new MeasureableRepairSession(parentRepairSession, UUIDGen.getTimeUUID(), fullRange,
                                                    KEYSPACE, RepairParallelism.SEQUENTIAL, neighbors,
                                                    ActiveRepairService.UNREPAIRED_SSTABLE, CF);

        this.job = new RepairJob(session, CF);
        this.sessionJobDesc = new RepairJobDesc(session.parentRepairSession, session.getId(),
                                                session.keyspace, CF, session.getRanges());

        DatabaseDescriptor.setBroadcastAddress(addr1);
    }

    public void reset()
    {
        ActiveRepairService.instance.terminateSessions();
        MessagingService.instance().clearMessageSinks();
    }



    /**
     * CASSANDRA-15902: Verify that repair job will be released after force shutdown on the session
     */
    @Test
    public void releaseThreadAfterSessionForceShutdown() throws Throwable
    {
        
        for (int numNeighbors=5;numNeighbors<=50;numNeighbors+=10){
            System.out.println("!!!numNeighbors: " + numNeighbors);
            
        
            Map<InetAddress, MerkleTrees> mockTrees = new HashMap<>();
            mockTrees.put(FBUtilities.getBroadcastAddress(), createInitialTree(false));
            
            
            setup(numNeighbors);
            for (int i=2; i<=1+numNeighbors; i++){
                mockTrees.put(getAddr(i), createInitialTree(false));
            }

            List<MessageOut> observedMessages = new ArrayList<>();
            interceptRepairMessages(mockTrees, observedMessages);

            session.simulateValidationsOutstanding();

            Thread jobThread = new Thread(() -> job.run());
            jobThread.start();

            session.waitUntilReceivedFirstValidationComplete();

            session.forceShutdown(new Exception("force shutdown for testing"));

            jobThread.join(TimeUnit.SECONDS.toMillis(TEST_TIMEOUT_S));

            long repairJobSize = ObjectSizes.measureDeep(job);


            System.out.println("!!!jobSize: " + repairJobSize);
            reset();
        }
    }
    

    private MerkleTrees createInitialTree(boolean invalidate)
    {
        MerkleTrees tree = new MerkleTrees(MURMUR3_PARTITIONER);
        tree.addMerkleTrees((int) Math.pow(2, 15), fullRange);
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }

        if (invalidate)
        {
            // change a range in one of the trees
            Token token = MURMUR3_PARTITIONER.midpoint(fullRange.get(0).left, fullRange.get(0).right);
            tree.invalidate(token);
            tree.get(token).hash("non-empty hash!".getBytes());
        }

        return tree;
    }

    private void interceptRepairMessages(Map<InetAddress, MerkleTrees> mockTrees,
                                         List<MessageOut> messageCapture)
    {
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                if (message == null || !(message.payload instanceof RepairMessage))
                    return false;

                // So different Thread's messages don't overwrite each other.
                synchronized (messageLock)
                {
                    messageCapture.add(message);
                }

                RepairMessage rm = (RepairMessage) message.payload;
                switch (rm.messageType)
                {
                    case SNAPSHOT:
                        MessageIn<?> messageIn = MessageIn.create(to, null,
                                                                  Collections.emptyMap(),
                                                                  MessagingService.Verb.REQUEST_RESPONSE,
                                                                  MessagingService.current_version);
                        MessagingService.instance().receive(messageIn, id, System.currentTimeMillis(), false);
                        break;
                    case VALIDATION_REQUEST:
                        session.validationComplete(sessionJobDesc, to, mockTrees.get(to));
                        break;
                    case SYNC_REQUEST:
                        SyncRequest syncRequest = (SyncRequest) rm;
                        session.syncComplete(sessionJobDesc, new NodePair(syncRequest.src, syncRequest.dst), true);
                        break;
                    default:
                        break;
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return message.verb == MessagingService.Verb.REQUEST_RESPONSE;
            }
        });
    }
}
