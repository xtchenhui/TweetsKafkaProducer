import twitter4j.conf.ConfigurationBuilder;

public class ConfigurationProvider {

    public static ConfigurationBuilder getConfig(){
        String custkey, custsecret;
        String accesstoken, accesssecret;
        custkey = "";
        custsecret="";
        accesstoken ="";
        accesssecret =""; 

        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret)

                ;
        return config;
    }
}
