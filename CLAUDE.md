## Commands
use "py" to use python
use "rm" for removing files

## Assumptions
NEVER assume I want you to do more. do only exactly as I said. If any code changes are out of scope mentioned, ALWAYS confirm with user first before making any change.

## General Coding Standards
Stricly follow Don't Repeat Yourself principle
Strictly follow Single Responsility Principle 

## ALWAYS reorganize so that:

In General:
Really THINK about the below.
Remove any unused variables and references.
All exception end the entire script and logs to console.
supabase saving functions always assume data being passed is in correct format and data type. NO DATA VALIDATION NEEDED HERE.
Ensure to keep consistent property names for listings parsed out. 
DO NOT TRACK ADDITIONAL INFORMATION FOR METADATA TRACKING OR STATISTICS TO DISPLAY.
perform each main sections in batches. especially fetching or saving to database.

Cireba:
main function is broken into three main section with try-catch blocks -> fetching crawled data, parsing, and saving to supabase
fetching -> same fetch logic currently using. fail on any page that we weren't able to get! 
parsing -> parse markdown, and perform data type validation, cleaning, currency conversion. then returned parsed listings.
saving to supabase -> simply pass the parsed listings. its cleaned so pass as is for saving.

Ecaytrade:
main function is broken into four  main section with try-catch blocks -> fetching crawled data, parsing, removing mls listings, and saving to supabase
fetching -> same fetch logic currently using. fail on any page that we weren't able to get! 
parsing -> parse markdown, and perform data type validation, cleaning, currency conversion. then returned parsed listings.
removing mls listings -> perform current logic of filtering out any listings that fuzz match name and exact match price with listings in cireba.
saving to supabase -> simply pass the parsed listings. its cleaned so pass as is for saving.
