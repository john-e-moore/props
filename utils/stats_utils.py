from scipy.optimize import minimize_scalar, fsolve
from scipy.stats import poisson, norm, expon, lognorm, gamma


def american_odds_to_probability(odds):
    """
    Convert American betting odds to the corresponding probability.
    
    Parameters:
    odds (int): The American odds value. Can be positive or negative.

    Returns:
    float: The probability of the event occurring, as calculated from the odds.
    """
    if odds > 0:
        # For positive odds
        probability = 100 / (odds + 100)
    else:
        # For negative odds
        probability = -odds / (-odds + 100)
    
    return probability

def calculate_vig_free_odds_and_vig(over_odds, over_under, under_odds):
    # Convert American odds to decimal odds
    over_decimal = (abs(over_odds) / 100 + 1) if over_odds > 0 else (100 / abs(over_odds) + 1)
    under_decimal = (abs(under_odds) / 100 + 1) if under_odds > 0 else (100 / abs(under_odds) + 1)

    # Calculate implied probabilities
    over_prob = 1 / over_decimal
    under_prob = 1 / under_decimal

    # Calculate vig-free odds
    over_vig_free = over_prob / (over_prob + under_prob)
    under_vig_free = under_prob / (over_prob + under_prob)

    # Calculate the vig
    vig = over_prob + under_prob - 1

    # Return the percent chance of the 'over' occurring and the vig
    return (over_vig_free if over_under == 'over' else under_vig_free, vig)

def find_normal_mean(n, over_odds, under_odds, sigma):
    """
    Calculate the mean of a normally distributed process given:
    n: value such that P(X < n) = p_less
    p_less: probability that the distribution is less than n
    
    """
    print(f"n: {n}")
    print(f"Over odds: {over_odds}")
    print(f"Under odds: {under_odds}")
    p_less = calculate_vig_free_odds_and_vig(over_odds, 'under', under_odds)[0]
    #print(f"Vig free Under odds: {vig_free_under_odds}")
    #p_less = american_odds_to_probability(vig_free_under_odds)

    print(f"P less: {p_less}")
    print(f"Sigma: {sigma}")
    
    # Inverse CDF (probit function) to find z-score
    z = norm.ppf(p_less)

    print(f"z: {z}")
    
    # Calculate mean
    mean = n - sigma * z

    print(f"Mean: {mean}")
    
    return mean

def poisson_mean_from_market(
        X: float, # Market over/under number
        over_american: int,
        under_american: int,
        ):
    """
    Find the lambda (mu, aka mean) of a Poisson distribution given X and the target cumulative probability.

    Used to calculate mean for counting stats like receptions, rushing attempts, touchdowns.
    """

    # Calculate under odds from American market
    target_prob = calculate_vig_free_odds_and_vig(over_american, 'under', under_american)[0]
    
    # Objective function to minimize
    def objective(lam):
        return (poisson.cdf(X, lam) - target_prob)**2
    
    # Minimize the objective function
    result = minimize_scalar(objective, bounds=(0, 50), method='bounded')
    
    return result.x

def calculate_gamma_scale(num_series: list) -> float:
    """
    :params:
        num_series: a list of weekly scores.

    :returns:
        The scale parameter for the gamma distribution.
    """
    return gamma.fit(num_series)[2]

def gamma_mean_from_market(
        X: float, # Market over/under number
        over_american: int,
        under_american: int, 
        scale: float):
    """
    Solve for the shape parameter alpha of the Gamma distribution given a target CDF value and target value,
    then compute the mean of the Gamma distribution using the solved alpha and provided scale.

    Used to model a sequence of Poisson events, i.e. receiving or rushing yards in a game.
    """

    # Calculate target cdf based on market odds
    target_cdf = calculate_vig_free_odds_and_vig(over_american, 'under', under_american)[0] # p(lower than x)

    # Define the function to find the root for
    def equation(alpha):
        return gamma.cdf(X, alpha, scale=scale) - target_cdf
    
    # Solve for alpha using fsolve
    alpha_solved = fsolve(equation, 1)[0]  # Initial guess for alpha is 1
    
    # Compute the mean
    mean = alpha_solved * scale
    
    # Compute over 100 prob for draftkings bonus
    prob_greater_than_100 = gamma_over_100_prob(alpha_solved, scale)
    
    return (mean, prob_greater_than_100)

def lambda_from_prob(X, target_prob):
    """
    Find the lambda (mu) of a Poisson distribution given X and the target cumulative probability.

    Used to calculate mean for counting stats like receptions, rushing attempts, touchdowns.
    """
    
    # Objective function to minimize
    def objective(lam):
        return (poisson.cdf(X, lam) - target_prob)**2
    
    # Minimize the objective function
    result = minimize_scalar(objective, bounds=(0, 50), method='bounded')
    
    return result.x

def gamma_over_100_prob(alpha, scale):
    """
    Given alpha and scale for a gamma distribution, this function returns the probability of picking a value greater than 100.
    """
    # Calculate the cumulative distribution function value for 100
    cdf_value = gamma.cdf(100, alpha, scale=scale)
    
    # Since the CDF gives the probability that a random variable is less than or equal to a certain value,
    # we subtract the CDF value from 1 to get the probability that the random variable is greater than the value.
    prob_greater_than_100 = 1 - cdf_value
    
    return prob_greater_than_100

def gamma_mean_for_cdf_value(target_cdf, target_value, scale):
    """
    Solve for the shape parameter alpha of the Gamma distribution given a target CDF value and target value,
    then compute the mean of the Gamma distribution using the solved alpha and provided scale.

    Used to model a sequence of Poisson events, i.e. receiving or rushing yards in a game.
    """
    # Define the function to find the root for
    def equation(alpha):
        return gamma.cdf(target_value, alpha, scale=scale) - target_cdf
    
    # Solve for alpha using fsolve
    alpha_solved = fsolve(equation, 1)[0]  # Initial guess for alpha is 1
    
    # Compute the mean
    mean = alpha_solved * scale

    # Compute over 100 prob for draftkings bonus
    prob_greater_than_100 = gamma_over_100_prob(alpha_solved, scale)
    
    return (mean, prob_greater_than_100)

def fit_normal_to_qb_data(df):
    """
    Fit a Normal distribution to the passing yards of quarterbacks.
    
    Parameters:
    - dataframe: A Pandas dataframe containing weekly player data
    
    Returns:
    - mu, sigma: Parameters of the fitted Normal distribution (mean and standard deviation)
    """
    # Filter to include only quarterbacks with 10 or more pass attempts
    filtered_data = df[(df['position'] == 'QB') & (df['attempts'] >= 10)]
    
    # Fit a Normal distribution to the passing yards data
    mu, sigma = norm.fit(filtered_data['passing_yards'])
    
    return mu, sigma

def evaluate_normal_distribution(normal_params, X, over_american, under_american):
    """
    Evaluate a Normal distribution for a given probability and X value.
    
    Parameters:
    - normal_params: Parameters of the Normal distribution (mean and standard deviation)
    - X: A value representing passing yards
    - p: Probability of passing for more than X yards
    
    Returns:
    - mean_passing_yards: Expected passing yards for a player with probability p of passing for more than X yards
    - prob_more_than_300: Probability of passing for more than 300 yards based on the Normal distribution
    """
    mu, sigma = normal_params
    p = calculate_vig_free_odds_and_vig(over_american, 'over', under_american)[0] # p(greater than x)
    
    # Determine the mean passing yards for a player with a probability p of passing for more than X yards
    def equation(mean):
        return norm.cdf(X, mean, sigma) - (1 - p)
    
    mean_passing_yards = fsolve(equation, mu)[0]
    
    # Calculate the probability of passing for more than 300 yards
    prob_more_than_300 = 1 - norm.cdf(300, mean_passing_yards, sigma)
    
    return mean_passing_yards, prob_more_than_300