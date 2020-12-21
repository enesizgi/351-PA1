package ceng.ceng351.cengtubedb;

import java.sql.*;
import java.util.ArrayList;

public class CengTubeDB implements ICengTubeDB{

    private String userName = "e231014";
    private String pass = "f0e94619";
    private String dbname = "db231014";
    private String host = "144.122.71.168";
    private int port = 3306;

    private Connection con = null;


    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.dbname + "?autoReconnect=true&useSSL=false";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.con =  DriverManager.getConnection(url, this.userName, this.pass);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public int createTables() {
        int temp = 0;

        String sql = "CREATE TABLE IF NOT EXISTS User(" +
                "userID INT NOT NULL, " +
                "userName VARCHAR(30), " +
                "email VARCHAR(30), " +
                "password VARCHAR(30), " +
                "status VARCHAR(15), " +
                "PRIMARY KEY (userID));";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(sql);
            temp++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "CREATE TABLE IF NOT EXISTS Video(" +
                "videoID INT NOT NULL, " +
                "userID INT, " +
                "videoTitle VARCHAR(60), " +
                "likeCount INT, " +
                "dislikeCount INT, " +
                "datePublished DATE, " +
                "FOREIGN KEY (userID) " +
                    "REFERENCES User(userID) " +
                    "ON DELETE CASCADE, " +
                "PRIMARY KEY (videoID));";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(sql);
            temp++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "CREATE TABLE IF NOT EXISTS Comment(" +
                "commentID INT NOT NULL, " +
                "userID INT, " +
                "videoID INT, " +
                "commentText VARCHAR(1000), " +
                "dateCommented DATE, " +
                "FOREIGN KEY (userID) " +
                    "REFERENCES User(userID) " +
                    "ON DELETE SET NULL, " +
                "FOREIGN KEY (videoID) " +
                    "REFERENCES Video(videoID) " +
                    "ON DELETE CASCADE, " +
                "PRIMARY KEY (commentID));";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(sql);
            temp++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "CREATE TABLE IF NOT EXISTS Watch(" +
                "userID INT NOT NULL, " +
                "videoID INT NOT NULL, " +
                "dateWatched DATE, " +
                "FOREIGN KEY (userID) " +
                    "REFERENCES User(userID) " +
                    "ON DELETE CASCADE, " +
                "FOREIGN KEY (videoID) " +
                    "REFERENCES Video(videoID) " +
                    "ON DELETE CASCADE , " +
                "PRIMARY KEY (userID,videoID));";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(sql);
            temp++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return temp;
    }

    @Override
    public int dropTables() {
        int temp = 0;
        /*
        String[] str = new String[4];
        str[0] = "User";
        str[1] = "Video";
        str[2] = "Comment";
        str[3] = "Watch";

        for (int i = 3;i>=0;i--) {
            String sql = "DROP TABLE IF EXISTS "+ str[i] +";";

            try {
                Statement statement = this.con.createStatement();
                statement.executeUpdate(sql);
                temp++;
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
*/
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return temp;
    }

    @Override
    public int insertUser(User[] users){
        int temp = 0;

        for (int i = 0;i<users.length;i++) {

            try {
            Statement statement = this.con.createStatement();

            String sql = String.format("INSERT INTO User VALUES (\"%d\",\"%s\",\"%s\",\"%s\",\"%s\");",
                    users[i].getUserID(),
                    users[i].getUserName(),
                    users[i].getEmail(),
                    users[i].getPassword(),
                    users[i].getStatus());

            statement.executeUpdate(sql);
            temp++;
            statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertVideo(Video[] videos) {
        int temp = 0;

        for (int i = 0;i<videos.length;i++) {

            try {
                Statement statement = this.con.createStatement();

                String sql = String.format("INSERT INTO Video VALUES (\"%d\",\"%d\",\"%s\",\"%d\",\"%d\",\"%s\");",
                        videos[i].getVideoID(),
                        videos[i].getUserID(),
                        videos[i].getVideoTitle(),
                        videos[i].getLikeCount(),
                        videos[i].getDislikeCount(),
                        videos[i].getDatePublished());

                statement.executeUpdate(sql);
                temp++;
                statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertComment(Comment[] comments) {
        int temp = 0;

        for (int i = 0;i<comments.length;i++) {

            try {
                Statement statement = this.con.createStatement();

                String sql = String.format("INSERT INTO Comment VALUES (\"%d\",\"%d\",\"%d\",\"%s\",\"%s\");",
                        comments[i].getCommentID(),
                        comments[i].getUserID(),
                        comments[i].getVideoID(),
                        comments[i].getCommentText(),
                        comments[i].getDateCommented());

                statement.executeUpdate(sql);
                temp++;
                statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertWatch(Watch[] watchEntries) {
        int temp = 0;

        for (int i = 0;i<watchEntries.length;i++) {

            try {
                Statement statement = this.con.createStatement();

                String sql = String.format("INSERT INTO Watch VALUES (\"%d\",\"%d\",\"%s\");",
                        watchEntries[i].getUserID(),
                        watchEntries[i].getVideoID(),
                        watchEntries[i].getDateWatched());

                statement.executeUpdate(sql);
                temp++;
                statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public QueryResult.VideoTitleLikeCountDislikeCountResult[] question3() {
        QueryResult.VideoTitleLikeCountDislikeCountResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT videoTitle,likeCount,dislikeCount FROM Video V " +
                    "WHERE V.likeCount>V.dislikeCount ORDER BY V.videoTitle ASC");
            ResultSet rs = statement.executeQuery(sql);
            ArrayList<String> arr1 = new ArrayList<String>();
            ArrayList<Integer> arr2 = new ArrayList<Integer>();
            ArrayList<Integer> arr3 = new ArrayList<Integer>();

            while(rs.next()) {
                arr1.add(rs.getString("videoTitle"));
                arr2.add(rs.getInt("likeCount"));
                arr3.add(rs.getInt("dislikeCount"));
            }


            tmp = new QueryResult.VideoTitleLikeCountDislikeCountResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.VideoTitleLikeCountDislikeCountResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public QueryResult.VideoTitleUserNameCommentTextResult[] question4(Integer userID) {
        QueryResult.VideoTitleUserNameCommentTextResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT U.userName,D.videoTitle,D.commentText FROM User U, " +
                    "(SELECT videoTitle,B.commentText FROM Video V, " +
                    "(SELECT videoID,commentText FROM Comment C WHERE C.userID = %d) B " +
                    "WHERE V.videoID = B.videoID) D WHERE U.userID = %d;",userID,userID);
            ResultSet rs = statement.executeQuery(sql);
            ArrayList<String> arr1 = new ArrayList<String>();
            ArrayList<String> arr2 = new ArrayList<String>();
            ArrayList<String> arr3 = new ArrayList<String>();

            while(rs.next()) {
                arr1.add(rs.getString("videoTitle"));
                arr2.add(rs.getString("userName"));
                arr3.add(rs.getString("commentText"));
            }


            tmp = new QueryResult.VideoTitleUserNameCommentTextResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.VideoTitleUserNameCommentTextResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public QueryResult.VideoTitleUserNameDatePublishedResult[] question5(Integer userID) {
        QueryResult.VideoTitleUserNameDatePublishedResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT V2.videoTitle, U1.userName, V2.datePublished " +
                    "FROM User U1, Video V2 WHERE V2.userID = %d AND U1.userID = %d " +
                    "AND V2.datePublished IN (SELECT MIN(V1.datePublished) " +
                    "FROM Video V1 WHERE V1.userID = %d AND V1.videoTitle NOT LIKE \"%%VLOG%%\");",userID,userID,userID);

            ResultSet rs = statement.executeQuery(sql);
            ArrayList<String> arr1 = new ArrayList<String>();
            ArrayList<String> arr2 = new ArrayList<String>();
            ArrayList<String> arr3 = new ArrayList<String>();

            while(rs.next()) {
                arr1.add(rs.getString("videoTitle"));
                arr2.add(rs.getString("userName"));
                arr3.add(rs.getString("datePublished"));
            }


            tmp = new QueryResult.VideoTitleUserNameDatePublishedResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.VideoTitleUserNameDatePublishedResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public QueryResult.VideoTitleUserNameNumOfWatchResult[] question6(String dateStart, String dateEnd) {
        QueryResult.VideoTitleUserNameNumOfWatchResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT V.videoTitle,U.userName,C.count FROM Video V, User U, " +
                    "(SELECT W.videoID, COUNT(*) AS count FROM Watch W WHERE W.dateWatched >= '%s' " +
                    "AND W.dateWatched <= '%s' GROUP BY W.videoID ORDER BY count DESC LIMIT 3) C " +
                    "WHERE V.userID = U.userID AND V.videoID = C.videoID;",dateStart,dateEnd);

            ResultSet rs = statement.executeQuery(sql);
            ArrayList<String> arr1 = new ArrayList<String>();
            ArrayList<String> arr2 = new ArrayList<String>();
            ArrayList<Integer> arr3 = new ArrayList<Integer>();

            while(rs.next()) {
                arr1.add(rs.getString("videoTitle"));
                arr2.add(rs.getString("userName"));
                arr3.add(rs.getInt("count"));
            }


            tmp = new QueryResult.VideoTitleUserNameNumOfWatchResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.VideoTitleUserNameNumOfWatchResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public QueryResult.UserIDUserNameNumOfVideosWatchedResult[] question7() {
        QueryResult.UserIDUserNameNumOfVideosWatchedResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT U.userID,U.userName,B.count FROM User U, " +
                    "(SELECT userID, COUNT(*) AS count FROM Watch GROUP BY userID) B WHERE U.userID = B.userID ORDER BY U.userID ASC;");
            ResultSet rs = statement.executeQuery(sql);
            ArrayList<Integer> arr1 = new ArrayList<Integer>();
            ArrayList<String> arr2 = new ArrayList<String>();
            ArrayList<Integer> arr3 = new ArrayList<Integer>();

            while(rs.next()) {
                arr1.add(rs.getInt("userID"));
                arr2.add(rs.getString("userName"));
                arr3.add(rs.getInt("count"));
            }


            tmp = new QueryResult.UserIDUserNameNumOfVideosWatchedResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.UserIDUserNameNumOfVideosWatchedResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

     @Override
    public QueryResult.UserIDUserNameEmailResult[]  question8() {/*
         QueryResult.UserIDUserNameEmailResult[] tmp = null;
         try {
             Statement statement = this.con.createStatement();

             String sql = String.format("SELECT DISTINCT U.userID,U.userName,U.email FROM User U " +
                     "WHERE NOT EXISTS (SELECT V.videoID FROM Video V WHERE NOT EXISTS " +
                     "(SELECT W.videoID FROM Watch W WHERE W.videoID = V.videoID AND W.userID = U.userID)) " +
                     " " +
                     "ORDER BY U.userID ASC");
             ResultSet rs = statement.executeQuery(sql);
             ArrayList<Integer> arr1 = new ArrayList<Integer>();
             ArrayList<String> arr2 = new ArrayList<String>();
             ArrayList<String> arr3 = new ArrayList<String>();

             while(rs.next()) {
                 arr1.add(rs.getInt("userID"));
                 arr2.add(rs.getString("userName"));
                 arr3.add(rs.getString("email"));
             }


             tmp = new QueryResult.UserIDUserNameEmailResult[arr1.size()];

             for (int i = 0;i<arr1.size();i++) {
                 tmp[i] = new QueryResult.UserIDUserNameEmailResult(arr1.get(i),arr2.get(i),arr3.get(i));
             }

             statement.close();

         } catch (SQLException e) {
             e.printStackTrace();
         }*/
         return null;
    }

    @Override
    public QueryResult.UserIDUserNameEmailResult[]  question9() {
        QueryResult.UserIDUserNameEmailResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("SELECT userID,userName,email FROM User U " +
                    "WHERE U.userID IN (SELECT userID FROM Watch W WHERE W.userID = U.userID) " +
                    "AND U.userID NOT IN (SELECT userID FROM Comment C WHERE C.userID = U.userID) ORDER BY U.userID ASC;");
            ResultSet rs = statement.executeQuery(sql);
            ArrayList<Integer> arr1 = new ArrayList<Integer>();
            ArrayList<String> arr2 = new ArrayList<String>();
            ArrayList<String> arr3 = new ArrayList<String>();

            while(rs.next()) {
                arr1.add(rs.getInt("userID"));
                arr2.add(rs.getString("userName"));
                arr3.add(rs.getString("email"));
            }


            tmp = new QueryResult.UserIDUserNameEmailResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.UserIDUserNameEmailResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public int question10(int givenViewCount) {/*
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("UPDATE User U1 SET status=\"verified\" WHERE %d <= ALL (SELECT SUM(SELECT COUNT(*) FROM Watch W1 WHERE W1.userID = V1.userID) FROM Video V1 WHERE V1.userID = U1.userID) ",givenViewCount);
            statement.executeUpdate(sql);/*
            ArrayList<String> arr1 = new ArrayList<String>();
            ArrayList<Integer> arr2 = new ArrayList<Integer>();
            ArrayList<Integer> arr3 = new ArrayList<Integer>();

            while(rs.next()) {
                arr1.add(rs.getString("videoTitle"));
                arr2.add(rs.getInt("likeCount"));
                arr3.add(rs.getInt("dislikeCount"));
            }


            tmp = new QueryResult.VideoTitleLikeCountDislikeCountResult[arr1.size()];

            for (int i = 0;i<arr1.size();i++) {
                tmp[i] = new QueryResult.VideoTitleLikeCountDislikeCountResult(arr1.get(i),arr2.get(i),arr3.get(i));
            }

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }*/
        return 0;
    }

    @Override
    public int question11(Integer videoID, String newTitle) {
        int temp = 0;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("UPDATE Video SET videoTitle =\"%s\" WHERE videoID =%d ;",newTitle,videoID);

            statement.executeUpdate(sql);
            temp++;
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return temp;
    }

    @Override
    public int question12(String videoTitle) {
        int temp = 0;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("DELETE FROM Video WHERE videoTitle=\"%s\" ;",videoTitle);

            statement.executeUpdate(sql);
            temp++;
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
