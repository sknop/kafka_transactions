package common;

public record RegionCode(String identifier, String longName, String areaCode) {
    public static final RegionCode[] REGION_CODES = {
            new RegionCode("UK", "United Kingdom", "+49"),
            new RegionCode("DE", "Germany", "+49"),
            new RegionCode("FR", "France", "+33"),
            new RegionCode("IT", "Italy", "+39"),
            new RegionCode("UA", "Ukraine", "+380"),
            new RegionCode("SE", "Sweden", "+46"),
            new RegionCode("DK", "Denmark", "+45"),
            new RegionCode("NO", "Norway", "+47"),
            new RegionCode("PL", "Poland", "+48"),
            new RegionCode("IS", "Iceland", "+354"),
            new RegionCode("US", "United States of America", "+1"),
            new RegionCode("IE", "Ireland", "+353"),
            new RegionCode("IN", "India", "+91"),
            new RegionCode("HK", "Hong Kong", "+852")
    };

    public static RegionCode getRegion(int index) {
        return REGION_CODES[index % REGION_CODES.length];
    }
}