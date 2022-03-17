package search;

import cluster.management.ServiceRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.squareup.okhttp.Response;
import model.DocumentData;
import model.Result;
import model.SerializationUtils;
import model.Task;
import model.proto.SearchModel;
import networking.OnRequestHandler;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestHandler {
    private static final String ENDPOINT = "/search";
    private static final String BOOKS_DIRECTORY = "src/main/resources/books";
    private final ServiceRegistry workerServiceRegister;
    private final WebClient webClient;
    private final List<String> documents;

    public SearchCoordinator(ServiceRegistry workerServiceRegister, WebClient client) {
        this.workerServiceRegister = workerServiceRegister;
        this.webClient = client;
        this.documents = readDocumentsList();
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            SearchModel.Request request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);

            return response.toByteArray();
        } catch (InvalidProtocolBufferException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        return SearchModel.Response.getDefaultInstance().toByteArray();
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private SearchModel.Response createResponse(SearchModel.Request searchRequest) throws InterruptedException, KeeperException {
        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();
        System.out.println("Received search query: " + searchRequest.getSeachQuery());

        List<String> searchTerms = TFIDF.getWordsFromLine(searchRequest.getSeachQuery());
        List<String> workers = workerServiceRegister.getAllServiceAddress();

        if (workers.isEmpty()){
            System.out.println("No search workers currently availabel");
            return searchResponse.build();
        }

        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results, searchTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);
        return searchResponse.build();
    }

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> terms) {
        Map<String, DocumentData> allDocumentsResults = new HashMap<>();

        for (Result result : results){
            allDocumentsResults.putAll(result.getDocumentToDocumentData());
        }

        System.out.println("Calculating score for all documents...");
        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentSortedByScore(terms, allDocumentsResults);

        return sortDocumentsByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> sortDocumentsByScore(Map<Double, List<String>> scoreToDocuments) {
        List<SearchModel.Response.DocumentStats> sortedDocumentsStatsList = new ArrayList<>();

        scoreToDocuments.forEach((score, docs) -> {
            docs.forEach(doc -> {
                File documentPath = new File(doc);

                SearchModel.Response.DocumentStats documentStats = SearchModel.Response.DocumentStats
                        .newBuilder()
                        .setScore(score)
                        .setDocumentName(documentPath.getName())
                        .setDocumentSize(documentPath.length())
                        .build();

                sortedDocumentsStatsList.add(documentStats);
            });
        });

        return sortedDocumentsStatsList;
    }

    private List<Result> sendTasksToWorkers(List<String> workerEndpoints, List<Task> tasks) {
        CompletableFuture<Response>[] futures = new CompletableFuture[workerEndpoints.size()];
        for (int i=0; i<workerEndpoints.size(); i++) {
            String workerEndpoint = workerEndpoints.get(i);
            Task task = tasks.get(i);
            byte[] payload = SerializationUtils.serialize(task);

            futures[i] = webClient.sendAsyncTask(workerEndpoint, payload);
        }

        List<Result> results = new ArrayList<>();
        for (CompletableFuture<Response> future : futures) {
            try {
                Response response = future.get();
                Result result = (Result) SerializationUtils.deserialize(response.body().bytes());
                results.add(result);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(String.format("Received %d/%d results", results.size(), tasks.size()));
        return results;
    }

    public List<Task> createTasks(int numberOfWorkers, List<String> searchTerms) {
        List<List<String>> workerDocuments = splitDocumentList(numberOfWorkers, documents);

        List<Task> tasks = new ArrayList<>();

        for (List<String> documentsForWorker : workerDocuments) {
            Task task = new Task(searchTerms, documentsForWorker);
            tasks.add(task);
        }

        return tasks;
    }

    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> docs) {
        int numberOfDocumentsPerWorker = (docs.size() + numberOfWorkers - 1) / numberOfWorkers;

        List<List<String>> workersDocuments = new ArrayList<>();

        for (int i=0; i<numberOfWorkers; i++) {
            int firstDocumentIndex = i * numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, docs.size());

            if (firstDocumentIndex >= lastDocumentIndexExclusive)
                break;

            List<String> currentWorkerDocuments = new ArrayList<>(docs.subList(firstDocumentIndex, lastDocumentIndexExclusive));
            workersDocuments.add(currentWorkerDocuments);
        }
        return workersDocuments;
    }

    private static List<String> readDocumentsList() {
        String basePath = new File("").getAbsolutePath();
        File documentDirectory = new File(BOOKS_DIRECTORY);
        return Arrays.asList(documentDirectory.list())
                .stream()
                .map(documentName -> basePath + "/" + BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }
}
