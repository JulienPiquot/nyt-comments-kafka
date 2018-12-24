import au.com.bytecode.opencsv.CSVParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Comment implements Serializable {

    public static void main(String[] args) throws IOException {
        Stream<Comment> comments = commentStream();
        comments.forEach(System.out::println);
    }

    public static Comment deserialize(byte[] data) {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (Comment) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(Comment article) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(article);
            return os.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Comment> commentStream() throws IOException {
        String[] files = {"data/CommentsJan2017.csv",
                "data/CommentsFeb2017.csv",
                "data/CommentsMarch2017.csv",
                "data/CommentsApril2017.csv",
                "data/CommentsMay2017.csv",
                "data/CommentsJan2018.csv",
                "data/CommentsFeb2018.csv",
                "data/CommentsMarch2018.csv",
                "data/CommentsApril2018.csv"};
        Stream<Comment> cStream = Arrays.stream(files).flatMap(file -> {
            try {
                Scanner lineScanner = new Scanner(Paths.get(file));
                lineScanner.useDelimiter("\n");
                CSVParser csvParser = new CSVParser();
                String[] headers = csvParser.parseLine(lineScanner.next());
                final Spliterator<String> splt = Spliterators.spliterator(lineScanner, Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
                return StreamSupport.stream(splt, false).onClose(lineScanner::close).map(line -> {
                    try {
                        return parseLine(line, headers);
                    } catch (Exception e) {
                        System.out.println("fail to read " + file + " - corrupted line : [" + line + "]");
                        e.printStackTrace();
                        return null;
                        //throw new RuntimeException(e);
                    }
                }).filter(Objects::nonNull);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return cStream;
    }

    public static List<Comment> readComments() throws IOException {
        String[] files = {"data/CommentsJan2017.csv",
                "data/CommentsFeb2017.csv",
                "data/CommentsMarch2017.csv",
                "data/CommentsApril2017.csv",
                "data/CommentsMay2017.csv",
                "data/CommentsJan2018.csv",
                "data/CommentsFeb2018.csv",
                "data/CommentsMarch2018.csv",
                "data/CommentsApril2018.csv"};



        List<Comment> comments = new ArrayList<>();
        for (String file : files) {
            try (Reader br = Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8);
                 Scanner lineScanner = new Scanner(br)) {
                lineScanner.useDelimiter("\n");
                CSVParser csvParser = new CSVParser();
                String[] headers = csvParser.parseLine(lineScanner.next());
                int lineNb = 0;
                while (lineScanner.hasNext()) {
                    if (lineNb % 1000 == 0) {
                        System.out.println("read line " + lineNb);
                    }
                    String line = lineScanner.next();
                    try {
                        lineNb++;
                        comments.add(parseLine(line, headers));
                    } catch (Exception e) {
                        System.out.println("fail to read " + file + ":" + lineNb + " - corrupted line : [" + line + "]");
                        e.printStackTrace();
                    }

                }
            }
        }
        return comments;
    }

    private static Comment parseLine(String line, String[] headers) throws IOException {
        Map<String, String> commentMap = new HashMap<>();
        CSVParser csvParser = new CSVParser();
        String[] lineItem = csvParser.parseLine(line);
        for (int i = 0; i < lineItem.length; i++) {
            commentMap.put(headers[i], lineItem[i]);
        }
        Comment comment = new Comment();
        comment.approveDate = new Timestamp(1000 * Long.parseLong(commentMap.get("approveDate")));
        comment.commentBody = commentMap.get("commentBody");
        comment.commentID = (int) Double.parseDouble(commentMap.get("commentID"));
        comment.commentSequence = (int) Double.parseDouble(commentMap.get("commentSequence"));
        comment.commentType = commentMap.get("commentType");
        comment.createDate = new Timestamp(1000 * Math.round(Double.parseDouble(commentMap.get("createDate"))));
        comment.depth = (int) Double.parseDouble(commentMap.get("depth"));
        comment.editorsSelection = Boolean.parseBoolean(commentMap.get("editorsSelection"));
        comment.parentID = (int) Double.parseDouble(commentMap.get("parentID"));
        comment.recommandations = Integer.parseInt(Optional.ofNullable(commentMap.get("recommandations")).orElse("0"));
        comment.replyCount = Integer.parseInt(commentMap.get("replyCount"));
        comment.reportAbuseFlag = Boolean.parseBoolean(commentMap.get("reportAbuseFlag"));
        comment.sharing = Integer.parseInt(commentMap.get("sharing"));
        comment.status = commentMap.get("status");
        comment.timespeople = Integer.parseInt(commentMap.get("timespeople"));
        comment.trusted = Integer.parseInt(commentMap.get("trusted"));
        comment.userDisplayName = commentMap.get("userDisplayName");
        comment.useLocation = commentMap.get("useLocation");
        comment.articleID = commentMap.get("articleID");
        comment.newDesk = commentMap.get("newDesk");
        comment.articleWordCount = Integer.parseInt(commentMap.get("articleWordCount"));
        comment.printPage = Integer.parseInt(commentMap.get("printPage"));
        comment.typeOfMaterial = commentMap.get("typeOfMaterial");
        return comment;

    }

    private Timestamp approveDate;
    private String commentBody;
    private int commentID;
    private int commentSequence;
    private String commentType;
    private Timestamp createDate;
    private int depth;
    private boolean editorsSelection;
    private int parentID;
    private int recommandations;
    private int replyCount;
    private boolean reportAbuseFlag;
    private int sharing;
    private String status;
    private int timespeople;
    private int trusted;
    private String userDisplayName;
    private String useLocation;
    private String articleID;
    private String newDesk;
    private int articleWordCount;
    private int printPage;
    private String typeOfMaterial;

    public Timestamp getApproveDate() {
        return approveDate;
    }

    public String getCommentBody() {
        return commentBody;
    }

    public int getCommentID() {
        return commentID;
    }

    public int getCommentSequence() {
        return commentSequence;
    }

    public String getCommentType() {
        return commentType;
    }

    public Timestamp getCreateDate() {
        return createDate;
    }

    public int getDepth() {
        return depth;
    }

    public boolean isEditorsSelection() {
        return editorsSelection;
    }

    public int getParentID() {
        return parentID;
    }

    public int getRecommandations() {
        return recommandations;
    }

    public int getReplyCount() {
        return replyCount;
    }

    public boolean isReportAbuseFlag() {
        return reportAbuseFlag;
    }

    public int getSharing() {
        return sharing;
    }

    public String getStatus() {
        return status;
    }

    public int getTimespeople() {
        return timespeople;
    }

    public int getTrusted() {
        return trusted;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public String getUseLocation() {
        return useLocation;
    }

    public String getArticleID() {
        return articleID;
    }

    public String getNewDesk() {
        return newDesk;
    }

    public int getArticleWordCount() {
        return articleWordCount;
    }

    public int getPrintPage() {
        return printPage;
    }

    public String getTypeOfMaterial() {
        return typeOfMaterial;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "approveDate=" + approveDate +
                ", commentBody='" + commentBody + '\'' +
                ", commentID=" + commentID +
                ", commentSequence=" + commentSequence +
                ", commentType='" + commentType + '\'' +
                ", createDate=" + createDate +
                ", depth=" + depth +
                ", editorsSelection=" + editorsSelection +
                ", parentID=" + parentID +
                ", recommandations=" + recommandations +
                ", replyCount=" + replyCount +
                ", reportAbuseFlag=" + reportAbuseFlag +
                ", sharing=" + sharing +
                ", status='" + status + '\'' +
                ", timespeople=" + timespeople +
                ", trusted=" + trusted +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", useLocation='" + useLocation + '\'' +
                ", articleID='" + articleID + '\'' +
                ", newDesk='" + newDesk + '\'' +
                ", articleWordCount=" + articleWordCount +
                ", printPage=" + printPage +
                ", typeOfMaterial='" + typeOfMaterial + '\'' +
                '}';
    }
}
