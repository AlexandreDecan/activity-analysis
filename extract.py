import logging
import tqdm 
import pandas 
import os 
import datetime 

from github import Github

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract repository data from Github')
    
    parser.add_argument('repository', type=str, help='owner/repository')
    parser.add_argument('--key', type=str, help='API key to use', required=True)
    parser.add_argument('--out', type=str, help='Output path', required=True)
    
    args = parser.parse_args()
    
    gh = Github(args.key)
    logger.info('Remaining API hits %s', gh.rate_limiting)
    logger.info('Reset time %s', datetime.datetime.fromtimestamp(int(gh.rate_limiting_resettime)).strftime('%Y-%m-%d %H:%M:%S'))
    repository = gh.get_repo(args.repository)
    
    path = os.path.join(args.out, repository.name)
    if not os.path.exists(path):
        os.makedirs(path)
    
    
    
    commits = repository.get_commits()
    issues = repository.get_issues(state='all')
    pulls = repository.get_pulls(state='all')
    
    if not os.path.exists(os.path.join(path, 'commits.csv')):
        commits_data = []
        for commit in tqdm.tqdm(commits, 'commits'):
            sha = commit.sha
            if commit.author:
                if hasattr(commit.author, 'login') and commit.author.login:
                    author = commit.author.login
                else:
                    author = '{} <{}>'.format(commit.author.name, commit.author.email)
            else:
                author = None
            author_date = commit.commit.author.date
            if commit.committer:
                if hasattr(commit.committer, 'login') and commit.committer.login:
                    committer = commit.committer.login
                else:
                    committer = '{} <{}>'.format(commit.committer.name, commit.committer.email)
            else:
                committer = None
            committer_date = commit.commit.committer.date
            
            commit_data = (sha, author, author_date, committer, committer_date)
            commits_data.append(commit_data)
            logger.debug('Commit %s', commit_data)
        
        logger.info('Remaining API hits %s', gh.rate_limiting)
        
        commits_df = pandas.DataFrame(
            columns=['sha', 'author', 'author_date', 'committer', 'committer_date'], 
            data=commits_data
        )
        commits_df.to_csv(os.path.join(path, 'commits.csv'), index=False, quoting=1)
    else:
        logger.info('Skipping commits')
    
    if not os.path.exists(os.path.join(path, 'issues.csv')):
        issues_data = []
        for issue in tqdm.tqdm(issues, 'issues'):
            number = issue.number
            created_by = issue.user.login
            created_at = issue.created_at
            state = issue.state
            closed_by = issue.closed_by.login if issue.closed_by else None
            closed_at = issue.closed_at
            
            issue_data = (number, created_by, created_at, state, closed_by, closed_at)
            issues_data.append(issue_data)
            logger.debug('Issue %s', issue_data)
        
        logger.info('Remaining API hits %s', gh.rate_limiting)
        
        issues_df = pandas.DataFrame(
            columns=['number', 'created_by', 'created_at', 'state', 'closed_by', 'closed_at'],
            data=issues_data
        )
        issues_df.to_csv(os.path.join(path, 'issues.csv'), index=False, quoting=1)
    else:
        logger.info('Skipping issues')
        
    if not os.path.exists(os.path.join(path, 'pulls.csv')):
        pulls_data = []
        for pull in tqdm.tqdm(pulls, 'pulls'):
            number = pull.number
            created_by = pull.user.login
            created_at = pull.created_at
            state = pull.state
            commits = pull.commits
            merged_by = pull.merged_by.login if pull.merged_by else None
            merged_at = pull.merged_at
            
            pull_data = (number, created_by, created_at, state, commits, merged_by, merged_at)
            pulls_data.append(pull_data)
            logger.debug('Pull %s', pull_data)
        
        logger.info('Remaining API hits %s', gh.rate_limiting)

        pulls_df = pandas.DataFrame(
            columns=['number', 'created_by', 'created_at', 'state', 'commits', 'merged_by', 'merged_at'], 
            data=pulls_data
        )
        pulls_df.to_csv(os.path.join(path, 'pulls.csv'), index=False, quoting=1)
    else:
        logger.info('Skipping pulls')
    
    if not os.path.exists(os.path.join(path, 'comments.csv')):
        comments_data = []
        for issue in tqdm.tqdm(issues, 'issue comments'):
            for comment in issue.get_comments():
                number = comment.id
                issue_number = issue.number
                created_by = comment.user.login
                created_at = comment.created_at
                
                comment_data = (number, issue_number, created_by, created_at)
                comments_data.append(comment_data)
                logger.debug('Comment %s', comment_data)
            
        logger.info('Remaining API hits %s', gh.rate_limiting)

        comments_df = pandas.DataFrame(
            columns=['number', 'issue', 'created_by', 'created_at'],
            data=comments_data
        )
        comments_df.to_csv(os.path.join(path, 'comments.csv'), index=False, quoting=1)
    else:
        logger.info('Skipping comments')
    
        
    
    
    
    
