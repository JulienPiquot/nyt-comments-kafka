import au.com.bytecode.opencsv.CSVParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

public class Comment {

    public static void main(String[] args) throws IOException {
        List<Comment> comments = readComments();
        System.out.println(comments.size());
    }

    public static List<Comment> readComments() throws IOException {
        String[] files = {"data/CommentsJan2017.csv"};
//                "data/CommentsFeb2017.csv",
//                "data/CommentsMarch2017.csv",
//                "data/CommentsApril2017.csv",
//                "data/CommentsMay2017.csv",
//                "data/CommentsJan2018.csv",
//                "data/CommentsFeb2018.csv",
//                "data/CommentsMarch2018.csv",
//                "data/CommentsApril2018.csv"};

        List<Comment> comments = new ArrayList<>();
        CSVParser csvParser = new CSVParser();
        for (String file : files) {
            try (Scanner lineScanner = new Scanner(Paths.get(file))) {
                lineScanner.useDelimiter("\r\n");
                String[] headers = csvParser.parseLine(lineScanner.next());
                int lineNb = 0;
                while (lineScanner.hasNext()) {
                    String line = lineScanner.next();
                    Map<String, String> commentMap = new HashMap<>();
                    try {
                        lineNb++;
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
                        comment.createDate = new Timestamp(1000 * Long.parseLong(commentMap.get("createDate")));
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
                        comments.add(comment);
                    } catch (Exception e) {
                        System.out.println("fail to read " + file + ":" + lineNb + " - corrupted line : [" + line + "]");
                    }
                }
            }
        }
        return comments;
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
