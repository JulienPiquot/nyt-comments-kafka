import au.com.bytecode.opencsv.CSVParser;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

public class Article implements Serializable {

    public static void main(String[] args) throws IOException {
        List<Article> articles = readArticles();
        System.out.println(articles);
    }

    public static List<Article> readArticles() throws IOException {
        String[] files = {"data/ArticlesJan2017.csv",
                "data/ArticlesFeb2017.csv",
                "data/ArticlesMarch2017.csv",
                "data/ArticlesApril2017.csv",
                "data/ArticlesMay2017.csv",
                "data/ArticlesJan2018.csv",
                "data/ArticlesFeb2018.csv",
                "data/ArticlesMarch2018.csv",
                "data/ArticlesApril2018.csv"};

        List<Article> articles = new ArrayList<>();
        CSVParser csvParser = new CSVParser();
        for (String file : files) {
            List<String> allLines = Files.readAllLines(Paths.get(file));
            String[] headers = csvParser.parseLine(allLines.get(0));
            for (String line : allLines.subList(1, allLines.size())) {
                Map<String, String> articleMap = new HashMap<>();
                String[] lineItem = csvParser.parseLine(line);
                for (int i = 0; i < lineItem.length; i++) {
                    articleMap.put(headers[i], lineItem[i]);
                }
                Article article = new Article();
                article.setArticleId(articleMap.get("articleID"));
                article.setArticleWordCount(Integer.parseInt(articleMap.get("articleWordCount")));
                article.setByline(articleMap.get("byline"));
                article.setDocumentType(articleMap.get("documentType"));
                article.setHeadline(articleMap.get("headline"));
                article.addKeywords(articleMap.get("keywords").replaceAll("^\\[", "")
                        .replaceAll("]$", "")
                        .replaceAll("'", "")
                        .split(","));
                article.setNewDesk(articleMap.get("newDesk"));
                article.setPubDate(Timestamp.valueOf(articleMap.get("pubDate")));
                article.setSectionName(articleMap.get("sectionName"));
                article.setSnippet(articleMap.get("snippet"));
                article.setSource(articleMap.get("source"));
                article.setTypeOfMaterial("typeOfMaterial");
                article.setWebURL("webURL");
                articles.add(article);
            }
        }
        return articles;
    }


    private String articleId;
    private int articleWordCount;
    private String byline;
    private String documentType;
    private String headline;
    private List<String> keywords = new ArrayList<>();
    private String newDesk;
    private int printPage;
    private Timestamp pubDate;
    private String sectionName;
    private String snippet;
    private String source;
    private String typeOfMaterial;
    private String webURL;

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public int getArticleWordCount() {
        return articleWordCount;
    }

    public void setArticleWordCount(int articleWordCount) {
        this.articleWordCount = articleWordCount;
    }

    public String getByline() {
        return byline;
    }

    public void setByline(String byline) {
        this.byline = byline;
    }

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void addKeywords(String[] keywords) {
        for (String keyword : keywords) {
            this.keywords.add(keyword.trim());
        }
    }

    public void addKeywords(List<String> keywords) {
        this.keywords.addAll(keywords);
    }

    public int getPrintPage() {
        return printPage;
    }

    public void setPrintPage(int printPage) {
        this.printPage = printPage;
    }

    public Timestamp getPubDate() {
        return pubDate;
    }

    public void setPubDate(Timestamp pubDate) {
        this.pubDate = pubDate;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getSnippet() {
        return snippet;
    }

    public void setSnippet(String snippet) {
        this.snippet = snippet;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTypeOfMaterial() {
        return typeOfMaterial;
    }

    public void setTypeOfMaterial(String typeOfMaterial) {
        this.typeOfMaterial = typeOfMaterial;
    }

    public String getWebURL() {
        return webURL;
    }

    public void setWebURL(String webURL) {
        this.webURL = webURL;
    }

    public String getNewDesk() {
        return newDesk;
    }

    public void setNewDesk(String newDesk) {
        this.newDesk = newDesk;
    }

    @Override
    public String toString() {
        return "Article{" +
                "articleId='" + articleId + '\'' +
                ", articleWordCount=" + articleWordCount +
                ", byline='" + byline + '\'' +
                ", documentType='" + documentType + '\'' +
                ", headline='" + headline + '\'' +
                ", keywords=" + keywords +
                ", newDesk='" + newDesk + '\'' +
                ", printPage=" + printPage +
                ", pubDate=" + pubDate +
                ", sectionName='" + sectionName + '\'' +
                ", snippet='" + snippet + '\'' +
                ", source='" + source + '\'' +
                ", typeOfMaterial='" + typeOfMaterial + '\'' +
                ", webURL='" + webURL + '\'' +
                '}';
    }
}
