import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.*;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;

public class MongoReactiveTest {
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collections;
    CountDownLatch latch;

    @BeforeEach
    public void before() {
        mongoClient = MongoClients.create("mongodb://localhost");
        database = mongoClient.getDatabase("mydb");
        collections = database.getCollection("test");
        latch = new CountDownLatch(1);
    }

    @Test
    public void insertOneDocumentTest() throws InterruptedException {
        Document document = new Document("name", "mongodb").append("type", "database").append("count", 1).append("info",
                new Document("x", 200).append("y", 102));
        Publisher<Success> publisher = collections.insertOne(document);
        publisher.subscribe(new Subscriber<Success>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Success success) {
                System.out.println("insert");
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void insertManyDocumentTest() throws InterruptedException {
        List<Document> documents = new ArrayList<>();
        for (int i=0;i<100;i++){
            documents.add(new Document("i",i));
        }
        Publisher<Success> publisher = collections.insertMany(documents);
        publisher.subscribe(new Subscriber<Success>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Success success) {
                System.out.println("insert many");
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete many");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findCollectionTest() throws InterruptedException {
        Publisher<Document> publisher = collections.find().first();
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document "+ document);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findCollectionWithFilterEQ() throws InterruptedException {
        Publisher<Document>publisher = collections.find(eq("i",71)).first();
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document filter "+ document);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findCollectionWithFilterGT() throws InterruptedException {
        Publisher<Document>publisher = collections.find(gt("i",50));
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(100);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document filter GT "+ document);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findWithFilterGTAndLTECollections() throws InterruptedException {
        //funciona como un limite? si ajaj

        Publisher<Document>publisher = collections.find(and(gt("i",50),lte("i",53)));
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(100);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document filter GT and lte "+ document);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findWithSortCollections() throws InterruptedException {
        Publisher<Document>publisher = collections.find(exists("i")).sort(descending("i"));
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(100);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document filter sort "+ document);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void findWithProjectingFieldsCollections() throws InterruptedException {
        Publisher<Document> publisher = collections.find().projection(excludeId());
        publisher.subscribe(new Subscriber<Document>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(120);
            }

            @Override
            public void onNext(Document document) {
                System.out.println("get document filter projecting "+ document);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                System.out.println("fallo");
            }

            @Override
            public void onComplete() {
                System.out.println("complete document");
                latch.countDown();
            }
        });
        latch.await();

    }
    @Test
    public void updateCollections() throws InterruptedException {
        Publisher<UpdateResult> publisher = collections.updateOne(eq("i",10),new Document("$set",new Document("i",110)));
        publisher.subscribe(new Subscriber<UpdateResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(120);
            }

            @Override
            public void onNext(UpdateResult updateResult) {
                System.out.println("update result" + updateResult);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("complete update");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void updateAll() throws InterruptedException {
        Publisher<UpdateResult> publisher = collections.updateMany(lt("i",100),new Document("$inc",new Document("i",110)));
        publisher.subscribe(new Subscriber<UpdateResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(120);
            }

            @Override
            public void onNext(UpdateResult updateResult) {
                System.out.println("update result" + updateResult);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("complete update");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void deleteOneCollections() throws InterruptedException {
        Publisher<DeleteResult> publisher = collections.deleteOne(eq("i",110));
        publisher.subscribe(new Subscriber<DeleteResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(120);
            }

            @Override
            public void onNext(DeleteResult deleteResult) {
                System.out.println("delete result" + deleteResult);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("complete delete");
                latch.countDown();
            }
        });
        latch.await();
    }
    @Test
    public void deleteManyCollections() throws InterruptedException {
        Publisher<DeleteResult> publisher = collections.deleteMany(gte("i",100));
        publisher.subscribe(new Subscriber<DeleteResult>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(120);
            }

            @Override
            public void onNext(DeleteResult deleteResult) {
                System.out.println("delete result" + deleteResult);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("complete delete");
                latch.countDown();
            }
        });
        latch.await();
    }

}
