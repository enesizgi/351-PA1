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
                    "ON DELETE CASCADE " +
                    "ON UPDATE CASCADE, " +
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
                    "ON DELETE SET NULL " +
                    "ON UPDATE CASCADE, " +
                "FOREIGN KEY (videoID) " +
                    "REFERENCES Video(videoID) " +
                    "ON DELETE CASCADE " +
                    "ON UPDATE CASCADE, " +
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
                "ON DELETE CASCADE " +
                "ON UPDATE CASCADE, " +
                "FOREIGN KEY (videoID) " +
                    "REFERENCES Video(videoID) " +
                "ON DELETE CASCADE  " +
                "ON UPDATE CASCADE , " +
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

        for (User user : users) {

            try {
                String sql = "INSERT INTO User VALUES (?,?,?,?,?);";


                PreparedStatement stat = con.prepareStatement(sql);
                stat.setInt(1, user.getUserID());
                stat.setString(2, user.getUserName());
                stat.setString(3, user.getEmail());
                stat.setString(4, user.getPassword());
                stat.setString(5, user.getStatus());

                stat.executeUpdate();
                temp++;
                stat.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertVideo(Video[] videos) {
        int temp = 0;

        for (Video video : videos) {

            try {

                String sql = "INSERT INTO Video VALUES (?,?,?,?,?,?);";


                PreparedStatement stat = con.prepareStatement(sql);
                stat.setInt(1, video.getVideoID());
                stat.setInt(2, video.getUserID());
                stat.setString(3, video.getVideoTitle());
                stat.setInt(4, video.getLikeCount());
                stat.setInt(5, video.getDislikeCount());
                stat.setString(6, video.getDatePublished());

                stat.executeUpdate();
                temp++;
                stat.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertComment(Comment[] comments) {
        int temp = 0;

        for (Comment comment : comments) {

            try {
                String sql = "INSERT INTO Comment VALUES (?,?,?,?,?);";

                PreparedStatement stat = con.prepareStatement(sql);
                stat.setInt(1, comment.getCommentID());
                stat.setInt(2, comment.getUserID());
                stat.setInt(3, comment.getVideoID());
                stat.setString(4, comment.getCommentText());
                stat.setString(5, comment.getDateCommented());

                stat.executeUpdate();
                temp++;
                stat.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return temp;
    }

    @Override
    public int insertWatch(Watch[] watchEntries) {
        int temp = 0;

        for (Watch watchEntry : watchEntries) {
            try {
                String sql = "INSERT INTO Watch VALUES (?,?,?);";

                PreparedStatement stat = con.prepareStatement(sql);
                stat.setInt(1, watchEntry.getUserID());
                stat.setInt(2, watchEntry.getVideoID());
                stat.setString(3, watchEntry.getDateWatched());

                stat.executeUpdate();
                temp++;
                stat.close();

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

            String sql = "SELECT videoTitle,likeCount,dislikeCount FROM Video V " +
                    "WHERE V.likeCount>V.dislikeCount ORDER BY V.videoTitle ASC";
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
                    "WHERE V.videoID = B.videoID) D WHERE U.userID = %d ORDER BY D.videoTitle ASC;",userID,userID);
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
            String sql = "SELECT V2.videoTitle, U1.userName, V2.datePublished " +
                    "FROM User U1, Video V2 WHERE V2.userID = ? AND U1.userID = ? " +
                    "AND V2.datePublished IN (SELECT MIN(V1.datePublished) " +
                    "FROM Video V1 WHERE V1.userID = ? AND V1.videoTitle NOT LIKE ?) " +
                    "ORDER BY V2.videoTitle ASC;";

            PreparedStatement stat = con.prepareStatement(sql);
            stat.setInt(1,userID);
            stat.setInt(2,userID);
            stat.setInt(3,userID);
            stat.setString(4,"%VLOG%");


            ResultSet rs = stat.executeQuery();
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

            stat.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    @Override
    public QueryResult.VideoTitleUserNameNumOfWatchResult[] question6(String dateStart, String dateEnd) {
        QueryResult.VideoTitleUserNameNumOfWatchResult[] tmp = null;
        try {
            String sql = "SELECT V.videoTitle,U.userName,C.count FROM Video V, User U, " +
                    "(SELECT W.videoID, COUNT(*) AS count FROM Watch W WHERE W.dateWatched >= ? " +
                    "AND W.dateWatched <= ? GROUP BY W.videoID ORDER BY count DESC LIMIT 3) C " +
                    "WHERE V.userID = U.userID AND V.videoID = C.videoID " +
                    "ORDER BY C.count DESC;";

            PreparedStatement stat = con.prepareStatement(sql);
            stat.setString(1,dateStart);
            stat.setString(2,dateEnd);

            ResultSet rs = stat.executeQuery();
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

            stat.close();

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

            String sql = "SELECT U.userID,U.userName,count FROM User U, " +
                    "(SELECT W1.userID,COUNT(W1.userID) AS count FROM Watch W1 " +
                    "WHERE W1.videoID IN (SELECT A.videoID FROM " +
                    "(SELECT COUNT(W.videoID) AS count, W.videoID FROM Watch W GROUP BY W.videoID) A " +
                    "WHERE A.count = 1) GROUP BY W1.userID) B " +
                    "WHERE B.userID = U.userID ORDER BY U.userID ASC;";
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
    public QueryResult.UserIDUserNameEmailResult[]  question8() {
         QueryResult.UserIDUserNameEmailResult[] tmp = null;
         try {
             Statement statement = this.con.createStatement();

             String sql = "SELECT U2.userID,U2.userName,U2.email FROM User U2 WHERE U2.userID NOT IN " +
                     "(SELECT U.userID FROM User U, Video V WHERE U.userID = V.userID AND V.videoID NOT IN " +
                     "(SELECT W.videoID FROM Watch W WHERE W.userID = U.userID)) AND U2.userID IN " +
                     "(SELECT U3.userID FROM User U3 WHERE U3.userID NOT IN " +
                     "(SELECT U1.userID FROM User U1, Video V1 WHERE U1.userID = V1.userID AND U1.userID NOT IN " +
                     "(SELECT C.userID FROM Comment C WHERE C.userID = U1.userID AND C.videoID = V1.videoID))) AND " +
                     "U2.userID IN (SELECT DISTINCT V4.userID FROM Video V4) ORDER BY U2.userID ASC;";
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
    public QueryResult.UserIDUserNameEmailResult[]  question9() {
        QueryResult.UserIDUserNameEmailResult[] tmp = null;
        try {
            Statement statement = this.con.createStatement();

            String sql = "SELECT userID,userName,email FROM User U " +
                    "WHERE U.userID IN (SELECT userID FROM Watch W WHERE W.userID = U.userID) " +
                    "AND U.userID NOT IN (SELECT userID FROM Comment C " +
                    "WHERE C.userID = U.userID) ORDER BY U.userID ASC;";
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
    public int question10(int givenViewCount) {
        int temp = 0;
        try {
            Statement statement = this.con.createStatement();

            String sql = String.format("UPDATE User U1 SET status=\"verified\" WHERE userID IN " +
                    "(SELECT U3.userID FROM (SELECT U2.userID, SUM(U2.C) AS S FROM " +
                    "(SELECT U.userID,(SELECT COUNT(*) FROM Watch W WHERE W.videoID = V.videoID) AS C " +
                    "FROM User U, Video V WHERE U.userID = V.userID) U2 GROUP BY userID) U3 " +
                    "WHERE U3.S > %d) ",givenViewCount);

            temp = statement.executeUpdate(sql);

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return temp;
    }

    @Override
    public int question11(Integer videoID, String newTitle) {
        int temp = 0;
        try {
            Statement statement = this.con.createStatement();

            String sql = "UPDATE Video SET videoTitle =\""+newTitle+"\" WHERE videoID ="+videoID.toString()+" ;";

            temp = statement.executeUpdate(sql);
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
            String sql = "DELETE FROM Video WHERE videoTitle=? ;";

            PreparedStatement stat = con.prepareStatement(sql);
            stat.setString(1, videoTitle);

            stat.execute();

            sql = "SELECT COUNT(*) AS count FROM Video";

            ResultSet rs;
            stat.close();
            stat = con.prepareStatement(sql);
            rs = stat.executeQuery();

            if (rs.next()) {
                temp = rs.getInt("count");
            }

            stat.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
