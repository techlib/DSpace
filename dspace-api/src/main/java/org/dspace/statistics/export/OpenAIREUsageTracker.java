/*
 * OpenAIREUsageTracker.java
 *
 * Version: 0.2
 * Date: 2021-11-20
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * - Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */
package org.dspace.statistics.export;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.services.model.Event;
import org.dspace.usage.UsageEvent;
import org.dspace.usage.AbstractUsageEventListener;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.logging.log4j.Logger;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import com.google.gson.Gson;

/**
 * User: dpie (dpierrakos at gmail.com) Date: Time:
 */
public class OpenAIREUsageTracker extends AbstractUsageEventListener {

    /* Log4j logger */
    private static Logger log = LogManager.getLogger(OpenAIREUsageTracker.class);

    // Base URl of the OpenAIRE platform
    private String matomobaseUrl;

    // Matomo Site ID
    private String matomoSiteID;

    // Matomo IP Anonumization Bytes
    private int matomoIPAnonymizationBytes;

    // Matomo Site Authentication Token
    private String matomoTokenAuth;

    // Flag if Matomo is enabled for current installation. Might be disabled for
    // test instances e.g..
    private boolean matomoEnabled;

    // Flag if mising requests are stored in local DB for retry
    private boolean matomoRetry;

    // Flag if a proxy is in front of the DSpace instance
    private boolean useProxies;

    // The URL of this DSpace instance
    private String dspaceURL;

    // The host name of this DSpace instance
    private String dspaceHostName;

    // Async http client to prevent waiting for Matomo server
    private CloseableHttpAsyncClient httpClient;

    // Pooling connection manager for httpClient
    private PoolingNHttpClientConnectionManager connectionManager;

    // The time out for a single connection if Matomo is slow or unreachable.
    private static final int CONNECTION_TIMEOUT = 5 * 1000;

    // The number of connections per route
    private static final int NUMBER_OF_CONNECTIONS_PER_ROUTE = 100;

    // If there are more than MAX_NUMBER_OF_PENDING_CONNECTIONS waiting to be
    // served events wont be send to Matomo
    private static final int MAX_NUMBER_OF_PENDING_CONNECTIONS = 10;

    /**
     * Constructor to initialize the HTTP Client. We only need one per instance
     * as it is able to handle multiple requests by multiple Threads at once.
     */
    public OpenAIREUsageTracker() {

        // init the httpClient with custom number of connections and timeout
        try {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECTION_TIMEOUT)
                    .setSocketTimeout(CONNECTION_TIMEOUT).build();
            DefaultConnectingIOReactor ioreactor;
            ioreactor = new DefaultConnectingIOReactor();
            connectionManager = new PoolingNHttpClientConnectionManager(
                    ioreactor);
            connectionManager
                    .setDefaultMaxPerRoute(NUMBER_OF_CONNECTIONS_PER_ROUTE);
            httpClient = HttpAsyncClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .setConnectionManager(connectionManager)
                    .build();

            httpClient.start();
        } catch (Exception e) {
            log.error(
                    "OpenAIRE Matomo Usage Tracker couldn't be initialized. There will be no tracking until server restart.",
                    e);
            httpClient = null;
        }
    }

    /**
     * Reading OpenAIRE's Matomo configuration options
     */
    private void readConfiguration() {
        // OpenAIRE Matomo variables
        matomobaseUrl = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getProperty("openaire.matomo.trackerURL");
        matomoSiteID = DSpaceServicesFactory.getInstance()
                .getConfigurationService().getProperty("openaire.matomo.siteID");
        matomoTokenAuth = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getProperty("openaire.matomo.tokenAuth");
        matomoEnabled = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getPropertyAsType("openaire.matomo.enabled", true);
        matomoIPAnonymizationBytes = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getPropertyAsType("openaire.matomo.ipanonymizationbytes", 0);
        matomoRetry = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getPropertyAsType("openaire.matomo.retry", true);

        // DSpace variables
        useProxies = DSpaceServicesFactory.getInstance()
                .getConfigurationService()
                .getPropertyAsType("useProxies", false);
        dspaceURL = DSpaceServicesFactory.getInstance()
                .getConfigurationService().getProperty("dspace.ui.url");
        dspaceHostName = DSpaceServicesFactory.getInstance()
                .getConfigurationService().getProperty("dspace.ui.url");

    }

    /**
     * Records the usage event in OpenAIRE's Matomo platform
     *
     * @param event Instance of the usage event
     */
    @Override
    public void receiveEvent(final Event event) {

        if (!(event instanceof UsageEvent)) {
            return;
        }

        try {
            this.readConfiguration();
            if (!matomoEnabled || httpClient == null) {
                return;
            }
            if (connectionManager.getTotalStats()
                    .getPending() >= MAX_NUMBER_OF_PENDING_CONNECTIONS) {
                log.error(
                        "Event could not be sent to OpenAIRE Matomo due to insufficient available connections");
                return;
            }

            log.debug("Usage event received " + event.getName());

            UsageEvent ue = (UsageEvent) event;
            if (ue.getAction() == UsageEvent.Action.VIEW) {
                // Item Download Case
                if (ue.getObject().getType() == Constants.BITSTREAM) {
                    Bitstream bitstream = (Bitstream) ue.getObject();
                    if (bitstream.getBundles().size() > 0) {
                        Bundle bundle = bitstream.getBundles().get(0);
                        if (bundle.getItems().size() > 0) {
                            Item item = bundle.getItems().get(0);
                            this.logEvent(item, bitstream, ue.getRequest());
                        }
                    }
                }
                // Item View Case
                if (ue.getObject().getType() == Constants.ITEM) {
                    Item item = (Item) ue.getObject();
                    this.logEvent(item, null, ue.getRequest());
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * Builds the URI to send the event to the configured OpenAIRE Matomo
     * instance and sends the request.
     *
     * @param item      instance of an item event
     * @param bitstream instance of a bitstream event
     * @param request   HTTP request
     * @throws IOException, URISyntaxException
     */
    private void logEvent(final Item item, final Bitstream bitstream,
                          final HttpServletRequest request)
            throws IOException, URISyntaxException {

        URIBuilder builder = new URIBuilder(matomobaseUrl);
        //builder.setPath(matomobaseUrl);
        builder.addParameter("idsite", matomoSiteID);
        builder.addParameter("cip", this.getIPAddress(request));
        builder.addParameter("rec", "1");
        builder.addParameter("token_auth", matomoTokenAuth);
        builder.addParameter("action_name", item.getName());

        // Agent Information
        String agent = StringUtils
                .defaultIfBlank(request.getHeader("USER-AGENT"), "");
        builder.addParameter("ua", agent);

        // Referer Information
        String urlref = StringUtils.defaultIfBlank(request.getHeader("referer"),
                "");
        builder.addParameter("urlref", urlref);

        // Country information in case of IPAnonymization
        if (matomoIPAnonymizationBytes > 0 && matomoIPAnonymizationBytes < 4) {
            String country = "";
            try {
                Locale locale = request.getLocale();
                country = locale.getCountry();
            } catch (Exception e) {
                log.error("Cannot get locale", e);
            }
            builder.addParameter("country", country);
        }

        if (bitstream != null) {
            // Bitstream information in case of download event
            StringBuffer sb = new StringBuffer(dspaceURL);
            sb.append("/bitstream/handle/").append(item.getHandle())
                    .append("/");
            sb.append(bitstream.getName());
            builder.addParameter("url", sb.toString());
            builder.addParameter("download", sb.toString());
        } else {
            // Item information in case of Item view event
            builder.addParameter("url",
                    dspaceURL + "/handle/" + item.getHandle());
        }

        // Matomo Custom Variable for OAI-PMH ID tracking
        Gson gson = new Gson();
        Map<String, String[]> jsonMatomoCustomVars = new HashMap<>();
        String[] oaipmhID = new String[]{"oaipmhID",
                "oai:" + dspaceHostName + ":" + item.getHandle()};
        jsonMatomoCustomVars.put("1", oaipmhID);
        builder.addParameter("cvar", gson.toJson(jsonMatomoCustomVars));

        this.sendRequest(builder.build());
    }

    /**
     * Get the IP-Address from the given request. Handles cases where a Proxy is
     * involved and IP-Address anonymization. Not yet working with IPv6
     *
     * @param request
     * @return IP address
     * @throws UnknownHostException
     */
    private String getIPAddress(final HttpServletRequest request)
            throws UnknownHostException {
        String clientIP = request.getRemoteAddr();
        if (useProxies && request.getHeader("X-Forwarded-For") != null) {
            /* This header is a comma delimited list */
            for (String xfip : request.getHeader("X-Forwarded-For").split(",")) {
                /*
                 * proxy itself will sometime populate this header with the same
                 * value in remote address. ordering in spec is vague, we'll
                 * just take the last not equal to the proxy
                 */
                if (!request.getHeader("X-Forwarded-For").contains(clientIP)) {
                    clientIP = xfip.trim();
                }
            }
        }

        // IP anonymization case
        if (matomoIPAnonymizationBytes > 0 && matomoIPAnonymizationBytes < 4) {

            // Check IPv4 or IPv6
            InetAddress ipadress = InetAddress.getByName(clientIP);
            if (ipadress instanceof Inet6Address) {
                clientIP = "0.0.0.0";
            } else {
                switch (matomoIPAnonymizationBytes) {
                    case 1:
                        clientIP = clientIP.substring(0,
                                StringUtils.ordinalIndexOf(clientIP, ".", 3))
                                + ".0";
                        break;
                    case 2:
                        clientIP = clientIP.substring(0,
                                StringUtils.ordinalIndexOf(clientIP, ".", 2))
                                + ".0.0";
                        break;
                    case 3:
                        clientIP = clientIP.substring(0,
                                StringUtils.ordinalIndexOf(clientIP, ".", 1))
                                + ".0.0.0";
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Invalid IP bytes: " + matomoIPAnonymizationBytes);
                }
            }
        }

        return clientIP;
    }

    /**
     * Send the request to the given URI asynchronous. Ignores the result except
     * for a status code check.
     *
     * @param uri
     */
    private void sendRequest(final URI uri) {
        final HttpGet request = new HttpGet(uri);

        httpClient.execute(request, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(final HttpResponse response) {
                if (response.getStatusLine()
                        .getStatusCode() == HttpStatus.SC_OK) {
                    log.info("Sent usage event to OpenAIRE Matomo");
                } else {
                    log.error("Error sending request to OpenAIRE Matomo." + " -> "
                            + response.getStatusLine());
                    this.failed(new Exception());
                }
            }

            @Override
            public void failed(final Exception ex) {
                log.error("Error sending usage event to OpenAIRE Matomo");
                try {
                    if (matomoRetry) {
                        OpenAireUsageTrackerUnreported unreportedReq = new OpenAireUsageTrackerUnreported();
                        unreportedReq.storeRequest(uri.toString());
                        log.info("Missing request stored to local DB");
                    }
                } catch (Exception e) {
                    log.error("Error storing unreported request");
                }

            }

            @Override
            public void cancelled() {
                log.info("Request cancelled");
            }

        });
    }
}
