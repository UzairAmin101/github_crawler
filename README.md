# github_crawler
A github crawler for my assessment 


# Q/A:

**Q.** Write a list of what you would do differently if this were run to collect data on 500 million
repositories instead of 100,000.
**A.** First of all I would use more github tokens to crawl in parallel. Each "worker" will be working on a different non overlapping subset of repos to ensure maximum efficiency.

**Q.**: How will the schema evolve if you want to gather more metadata in the future, for example,
issues, pull requests, commits inside pull requests, comments inside PRs and issues, reviews on a
PR, CI checks, etc.? A PR can get 10 comments today and then 20 comments tomorrow.
Updating the DB with this new information should be an efficient operation (minimal rows
affected)

**A.** When adding new information we can easily add more tables, for example if we are to add commits or issues then we can add a table for each of them and have the node id as a foreign key. Things like comments can be stored in a jsonb Column in a de-normalized manner, i.e all the comments for a single Pull Request as an array of objects in a single column. This will lead to only one row being affected per PR no matter how many comments we have.