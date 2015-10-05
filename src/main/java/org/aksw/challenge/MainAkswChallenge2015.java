package org.aksw.challenge;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.aksw.commons.util.compress.MetaBZip2CompressorInputStream;
import org.aksw.commons.util.strings.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class MainAkswChallenge2015 {
    public static void main(String[] args) throws Exception {
        Resource queryLog = new ClassPathResource("trained_queries.txt.bz2");
        MetaBZip2CompressorInputStream in = new MetaBZip2CompressorInputStream(queryLog.getInputStream());

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String rawLine;
        while((rawLine = reader.readLine()) != null) {
            String queryStr = StringUtils.urlDecode(rawLine);
            System.out.println(queryStr);

        }
    }
}
