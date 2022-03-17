package search;

import cluster.management.ServiceRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.protobuf.InvalidProtocolBufferException;
import com.squareup.okhttp.Response;
import model.frontend.FrontendSearchRequest;
import model.frontend.FrontendSearchResponse;
import model.proto.SearchModel;
import networking.OnRequestHandler;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

public class UserSearchHandler implements OnRequestHandler {
    private static final String ENDPOINT = "/documents_search";
    private static final String DOCUMENTS_LOCATION = "books";
    private final ObjectMapper objectMapper;
    private final WebClient client;
    private final ServiceRegistry searchCoordinatorRegistry;

    public UserSearchHandler(ServiceRegistry searchCoordinatorRegistry) {
        this.searchCoordinatorRegistry = searchCoordinatorRegistry;
        this.client = new WebClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            FrontendSearchRequest frontendSearchRequest = objectMapper.readValue(requestPayload, FrontendSearchRequest.class);
            FrontendSearchResponse frontendSearchResponse = createFrontendResponse(frontendSearchRequest);
            return objectMapper.writeValueAsBytes(frontendSearchResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private FrontendSearchResponse createFrontendResponse(FrontendSearchRequest frontendSearchRequest) {
        SearchModel.Response searchClusterResponse = sendRequestToSearchCluster(frontendSearchRequest.getSearchQuery());

        List<FrontendSearchResponse.SearchResultInfo> filteredResults =
                filterResults(searchClusterResponse, frontendSearchRequest.getMaxNumberOfResults(), frontendSearchRequest.getMinScore());
        return new FrontendSearchResponse(filteredResults, DOCUMENTS_LOCATION);
    }

    private List<FrontendSearchResponse.SearchResultInfo> filterResults(SearchModel.Response searchClusterResponse,
                                                                        long maxResults,
                                                                        double minScore) {
        double maxScore = getMaxScore(searchClusterResponse);

        List<FrontendSearchResponse.SearchResultInfo> searchResultInfoList = new ArrayList<>();

        for( int i=0; i<searchClusterResponse.getRelevantDocumentsCount() && i < maxResults; i++) {
            int normalizedDoocumentScore = normalizeScore(searchClusterResponse.getRelevantDocuments(i).getScore(), maxScore);
            if (normalizedDoocumentScore < minScore) {
                break;
            }

            String documentName = searchClusterResponse.getRelevantDocuments(i).getDocumentName();
            String title = getDocumentTitle(documentName);
            String extension = getDocumentExtension(documentName);

            FrontendSearchResponse.SearchResultInfo resultInfo =
                    new FrontendSearchResponse.SearchResultInfo(title, extension, normalizedDoocumentScore);
            searchResultInfoList.add(resultInfo);
        }
        return searchResultInfoList;
    }

    private static String getDocumentExtension(String document) {
        String[] parts = document.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return "";
    }

    private static String getDocumentTitle(String document) {
        return document.split("\\.")[0];
    }

    private static int normalizeScore(double inputScore, double maxScore) {
        return (int) Math.ceil(inputScore * 100.0 / maxScore);
    }

    private static double getMaxScore(SearchModel.Response searchClusterResponse) {
        if (searchClusterResponse.getRelevantDocumentsCount() == 0) {
            return 0;
        }
        return searchClusterResponse.getRelevantDocumentsList()
                .stream()
                .map(document -> document.getScore())
                .max(Double::compareTo)
                .get();
    }

    private SearchModel.Response sendRequestToSearchCluster(String searchQuery) {
        SearchModel.Request searchRequest = SearchModel.Request.newBuilder()
                .setSeachQuery(searchQuery)
                .build();

        try {
            String coordinatorAddress = searchCoordinatorRegistry.getRandomServiceAddress();
            if (coordinatorAddress == null) {
                System.out.println("Search Cluster Coordinator is unavailable");
                return SearchModel.Response.getDefaultInstance();
            }

            Response response = client.sendAsyncTask(coordinatorAddress, searchRequest.toByteArray()).join();
            byte[] payloadBody = response.body().bytes();

            return SearchModel.Response.parseFrom(payloadBody);
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance();
        }
    }
}
