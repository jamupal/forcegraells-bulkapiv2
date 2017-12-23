package graells.bulkapiv2;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;

class ConexionSF {

    static final String GRANTSERVICE = "/services/oauth2/token?grant_type=password";

    private String loginAccessToken = null;
    private String loginInstanceUrl = null;
    private HttpPost httpPost = null;

    //Constructor
    public ConexionSF(String username, String password, String token, String loginURL, String clientId, String clientSecret) {

        HttpClient httpclient = HttpClientBuilder.create().build();

        // Assemble the login request URL

        String URLToLogin = loginURL + GRANTSERVICE + "&client_id=" + clientId + "&client_secret=" + clientSecret + "&username=" + username + "&password=" + password + token;

        System.out.println(URLToLogin);

        // Login requests must be POSTs
        httpPost = new HttpPost(URLToLogin);
        HttpResponse response = null;

        try {
            // Execute the login POST request
            response = httpclient.execute(httpPost);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // verify response is HTTP OK
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            System.out.println("Error de autenticacion : " + statusCode);
            // Error is in EntityUtils.toString(response.getEntity())
            return;
        }

        String getResult = null;
        try {
            getResult = EntityUtils.toString(response.getEntity());
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        JSONObject jsonObject = null;

        try {
            jsonObject = (JSONObject) new JSONTokener(getResult).nextValue();
            loginAccessToken = jsonObject.getString("access_token");
            loginInstanceUrl = jsonObject.getString("instance_url");
        } catch (JSONException jsonException) {
            jsonException.printStackTrace();
        }
    }

    //Getters
    public String getloginAccessToken() {
        return loginAccessToken;
    }

    public String getloginInstanceUrl() {
        return loginInstanceUrl;
    }

    public void ReleaseConnection() {
        this.httpPost.releaseConnection();
        System.out.println("ConexiÃ³n liberada");
    }
}