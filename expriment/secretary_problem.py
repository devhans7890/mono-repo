import random

def top_percent_simulation(n=10, simulations=100, top_percent=0.1):
    threshold = int(n / 2.71828)  # ≈ 37% of n
    success_count = 0
    top_k = int(n * top_percent)

    for _ in range(simulations):
        applicants = list(range(1, n + 1))  # 1등이 가장 우수
        random.shuffle(applicants)

        best_seen = min(applicants[:threshold])
        selected = None

        for i in range(threshold, n):
            if applicants[i] < best_seen:
                selected = applicants[i]
                break
        else:
            selected = applicants[-1]

        if selected <= top_k:
            success_count += 1

    success_rate = success_count / simulations
    success_rate_percentage = round(success_rate * 100, 2)
    print(success_rate_percentage)

if __name__=="__main__":
    top_percent_simulation()
